package com.github.codelionx.distod.actors

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PendingJobMap}

import scala.annotation.tailrec


object PartitionManager {

  private case class ProductComputed(key: CandidateSet, partition: StrippedPartition) extends PartitionCommand


  val name = "partition-manager"

  def apply(): Behavior[PartitionCommand] = Behaviors.setup(context => Behaviors.withStash(20) { stash =>
    new PartitionManager(context, stash).start()
  })

  private case class ComputeProductJob(
      key: CandidateSet,
      partitionA: Either[CandidateSet, StrippedPartition],
      partitionB: Either[CandidateSet, StrippedPartition]
  )

  private sealed trait PendingResponse
  private final case class PendingError(replyTo: ActorRef[ErrorFound]) extends PendingResponse
  private final case class PendingStrippedPartition(replyTo: ActorRef[StrippedPartitionFound]) extends PendingResponse

  private def partitionGenerator(jobs: Seq[ComputeProductJob], replyTo: ActorRef[ProductComputed]): Behavior[NotUsed] =
    Behaviors.setup { _ =>
      // actually just a stateful loop (but with immutable collections ;) )
      @tailrec
      def computePartition(
          partitions: Map[CandidateSet, StrippedPartition], remainingJobs: Seq[ComputeProductJob]
      ): Unit = {
        val job :: newRemainingJobs = remainingJobs
        val pA = job.partitionA match {
          case Right(p) => p
          case Left(candidate) => partitions(candidate)
        }
        val pB = job.partitionB match {
          case Right(p) => p
          case Left(candidate) => partitions(candidate)
        }
        val newPartition = (pA * pB).asInstanceOf[StrippedPartition]
        replyTo ! ProductComputed(job.key, newPartition)

        if (newRemainingJobs != Nil)
          computePartition(partitions + (job.key -> newPartition), newRemainingJobs)
      }

      computePartition(Map.empty, jobs)
      Behaviors.stopped
    }
}


class PartitionManager(context: ActorContext[PartitionCommand], stash: StashBuffer[PartitionCommand]) {

  import PartitionManager._


  def start(): Behavior[PartitionCommand] = initialize(Seq.empty, Map.empty)

  private val settings = Settings(context.system)

  private def initialize(
      attributes: Seq[Int], singletonPartitions: Map[CandidateSet, FullPartition]
  ): Behavior[PartitionCommand] = Behaviors.receiveMessage {

    case SetAttributes(newAttributes) =>
      context.log.info("Received attributes: {}", newAttributes)
      nextBehavior(newAttributes, singletonPartitions)

    case InsertPartition(key, value: FullPartition) if key.size == 1 =>
      context.log.info("Inserting full partition for key {}", key)
      val newSingletonPartitions = singletonPartitions + (key -> value)
      nextBehavior(attributes, newSingletonPartitions)

    case m =>
      context.log.debug("Stashing request {}", m)
      stash.stash(m)
      Behaviors.same
  }

  private def nextBehavior(
      attributes: Seq[Int], singletonPartitions: Map[CandidateSet, FullPartition]
  ): Behavior[PartitionCommand] =
    if (attributes.nonEmpty && singletonPartitions.size == attributes.size) {
      stash.unstashAll(
        behavior(attributes, singletonPartitions, Map.empty, PendingJobMap.empty)
      )
    } else {
      initialize(attributes, singletonPartitions)
    }

  private def behavior(
      attributes: Seq[Int],
      singletonPartitions: Map[CandidateSet, FullPartition], // keys of size == 1
      partitions: Map[CandidateSet, StrippedPartition], // keys of size != 1
      pendingJobs: PendingJobMap[CandidateSet, PendingResponse]
  ): Behavior[PartitionCommand] = Behaviors.receiveMessage {

    // inserts
    case SetAttributes(ignored) =>
      context.log.warn("Attempt to change attributes to {} after initialization step, ignoring!", ignored)
      Behaviors.same

    case InsertPartition(key, _: FullPartition) if key.size == 1 =>
      context.log.warn("Attempt to change initial partitions (key.size == 1) after initialization step, ignoring!")
      Behaviors.same

    case InsertPartition(key, value: StrippedPartition) =>
      context.log.info("Inserting partition for key {}", key)
      behavior(attributes, singletonPartitions, partitions + (key -> value), pendingJobs)

    // request attributes
    case LookupAttributes(replyTo) =>
      replyTo ! AttributesFound(attributes)
      Behaviors.same

    // single-attr keys
    case LookupError(key, replyTo) if key.size == 1 =>
      singletonPartitions.get(key) match {
        case Some(p) =>
          replyTo ! ErrorFound(key, p.stripped.error)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Full partition for key ${key} not found! Can not compute error.")
      }

    case LookupPartition(key, replyTo) if key.size == 1 =>
      singletonPartitions.get(key) match {
        case Some(p) =>
          replyTo ! PartitionFound(key, p)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Full partition for key ${key} not found!")
      }

    case LookupStrippedPartition(key, replyTo) if key.size == 1 =>
      singletonPartitions.get(key) match {
        case Some(p) =>
          replyTo ! StrippedPartitionFound(key, p.stripped)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Stripped partition for key ${key} not found!")
      }

    // rest
    case LookupError(key, replyTo) if key.size != 1 =>
      partitions.get(key) match {
        case Some(p: StrippedPartition) =>
          replyTo ! ErrorFound(key, p.error)
          Behaviors.same
        case None if pendingJobs.contains(key) =>
          context.log.debug("Error is being computed, queuing request for key {}", key)
          behavior(attributes, singletonPartitions, partitions, pendingJobs + (key -> PendingError(replyTo)))
        case None =>
          context.log.info("Partition to compute error not found. Starting background job to compute {}", key)
          val newJobs = generateStrippedPartitionJobs(
            key,
            singletonPartitions,
            partitions,
            pendingJobs,
            PendingError(replyTo)
          )
          behavior(attributes, singletonPartitions, partitions, newJobs)
      }

    case LookupPartition(key, _) if key.size != 1 =>
      throw new IllegalArgumentException(
        s"Only keys of size 1 contain full partitions, but your key has ${key.size} elements!"
      )

    case LookupStrippedPartition(key, replyTo) if key.size != 1 =>
      partitions.get(key) match {
        case Some(p: StrippedPartition) =>
          replyTo ! StrippedPartitionFound(key, p)
          Behaviors.same
        case None if pendingJobs.contains(key) =>
          context.log.debug("Partition is being computed, queuing request for key {}", key)
          behavior(attributes, singletonPartitions, partitions, pendingJobs + (key -> PendingStrippedPartition(replyTo)))
        case None =>
          context.log.info("Partition not found. Starting background job to compute {}", key)
          val newJobs = generateStrippedPartitionJobs(
            key,
            singletonPartitions,
            partitions,
            pendingJobs,
            PendingStrippedPartition(replyTo)
          )
          behavior(attributes, singletonPartitions, partitions, newJobs)
      }

    case ProductComputed(key, partition) =>
      context.log.debug("Received computed partition for key {}", key)
      val updatedPartitions = partitions + (key -> partition)
      pendingJobs.get(key) match {
        case Some(pendingResponses) => pendingResponses.foreach {
          case PendingStrippedPartition(ref) =>
            ref ! StrippedPartitionFound(key, partition)
          case PendingError(ref) =>
            ref ! ErrorFound(key, partition.error)
        }
        case None => // do nothing
      }
      behavior(attributes, singletonPartitions, updatedPartitions, pendingJobs.keyRemoved(key))
  }

  private def generateStrippedPartitionJobs(
      key: CandidateSet,
      singletonPartitions: Map[CandidateSet, FullPartition], otherPartitions: Map[CandidateSet, StrippedPartition],
      pendingJobs: PendingJobMap[CandidateSet, PendingResponse],
      pendingResponse: PendingResponse
  ): PendingJobMap[CandidateSet, PendingResponse] = {
    val partitions = otherPartitions ++ singletonPartitions.view.mapValues(_.stripped)
    val jobs = calcJobChain(key, partitions)
    context.spawnAnonymous(partitionGenerator(jobs, context.self), settings.cpuBoundTaskDispatcher)
    val x = jobs.map { job =>
      if (job.key == key)
        job.key -> Seq(pendingResponse)
      else
        job.key -> Seq.empty
    }
    pendingJobs ++ x
  }

  private def calcJobChain(
      key: CandidateSet,
      partitions: Map[CandidateSet, StrippedPartition]
  ): Seq[ComputeProductJob] = {

    def loop(subkey: CandidateSet): Seq[ComputeProductJob] = {
      val predecessorKeys = subkey.predecessors
      val foundKeys = predecessorKeys.filter(partitions.contains)
      val missingKeys = predecessorKeys.diff(foundKeys)

      if (foundKeys.size == 1) {
        val nextPred = missingKeys.head
        loop(nextPred) :+ ComputeProductJob(subkey, Right(partitions(foundKeys.head)), Left(nextPred))

      } else if (foundKeys.isEmpty) {
        val nextPred1 = missingKeys(0)
        val nextPred2 = missingKeys(1)
        loop(nextPred1) ++ loop(nextPred2) :+ ComputeProductJob(subkey, Left(nextPred1), Left(nextPred2))

      } else {
        val prePartitions = foundKeys.map(partitions).take(2)
        Seq(ComputeProductJob(subkey, Right(prePartitions(0)), Right(prePartitions(1))))
      }
    }

    loop(key)
  }
}
