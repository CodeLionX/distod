package com.github.codelionx.distod.actors

import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PendingJobMap}

import scala.annotation.tailrec


object PartitionManager {

  private case class ProductComputed(key: CandidateSet, partition: StrippedPartition) extends PartitionCommand


  def name = "partition-manager"

  def apply(): Behavior[PartitionCommand] = Behaviors.setup(new PartitionManager(_).start())

  private case class ComputeProductJob(
                                        key: CandidateSet,
                                        partitionA: Either[CandidateSet, StrippedPartition],
                                        partitionB: Either[CandidateSet, StrippedPartition]
                                      )

  private def partitionGenerator(jobs: Seq[ComputeProductJob], replyTo: ActorRef[ProductComputed]): Behavior[NotUsed] =
    Behaviors.setup { _ =>
      // actually just a stateful loop (but with immutable collections ;) )
      @tailrec
      def computePartition(partitions: Map[CandidateSet, StrippedPartition], remainingJobs: Seq[ComputeProductJob]): Unit = {
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


class PartitionManager(context: ActorContext[PartitionCommand]) {

  import PartitionManager._


  def start(): Behavior[PartitionCommand] = behavior(Map.empty, Map.empty, PendingJobMap.empty)

  private val settings = Settings(context.system)

  private def behavior(
                        singletonPartitions: Map[CandidateSet, FullPartition], // keys of size == 1
                        partitions: Map[CandidateSet, StrippedPartition], // keys of size != 1
                        pendingJobs: PendingJobMap[CandidateSet, ActorRef[StrippedPartitionFound]]
                      ): Behavior[PartitionCommand] = Behaviors.receiveMessage {

    case InsertPartition(key, value: FullPartition) if key.size == 1 =>
      context.log.info("Inserting full partition for key {}", key)
      behavior(singletonPartitions + (key -> value), partitions, pendingJobs)

    case InsertPartition(key, value: StrippedPartition) =>
      context.log.info("Inserting partition for key {}", key)
      behavior(singletonPartitions, partitions + (key -> value), pendingJobs)

    case LookupPartition(key, _) if key.size != 1 =>
      throw new IllegalArgumentException(
        s"Only keys of size 1 contain full partitions, but your key has ${key.size} elements!"
      )

    case LookupPartition(key, replyTo) =>
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

    case LookupStrippedPartition(key, replyTo) if key.size != 1 =>
      partitions.get(key) match {
        case Some(p: StrippedPartition) =>
          replyTo ! StrippedPartitionFound(key, p)
          Behaviors.same
        case None if pendingJobs.contains(key) =>
          context.log.info("Partition is being computed, queuing request for key {}", key)
          behavior(singletonPartitions, partitions, pendingJobs + (key -> replyTo))
        case None =>
          context.log.info("Partition not found. Starting background job to compute {}", key)
          val allPartitions = partitions ++ singletonPartitions.map(t => t._1 -> t._2.stripped)
          val jobs = pendingJobs ++ generateStrippedPartitions(key, allPartitions, replyTo)
          context.log.info("New pending jobs: {}", jobs)
          behavior(singletonPartitions, partitions, jobs)
      }

    case ProductComputed(key, partition) =>
      context.log.info("Received computed partition for key {}", key)
      val updatedPartitions = partitions + (key -> partition)
      pendingJobs.get(key) match {
        case Some(receiver) => receiver.foreach(ref =>
          ref ! StrippedPartitionFound(key, partition)
        )
        case None => // do nothing
      }
      behavior(singletonPartitions, updatedPartitions, pendingJobs.keyRemoved(key))
  }

  private def generateStrippedPartitions(
                                          key: CandidateSet,
                                          partitions: Map[CandidateSet, StrippedPartition],
                                          receiver: ActorRef[StrippedPartitionFound]
                                        ): PendingJobMap[CandidateSet, ActorRef[StrippedPartitionFound]] = {
    val jobs = calcJobChain(key, partitions)
    context.spawnAnonymous(partitionGenerator(jobs, context.self), settings.cpuBoundTaskDispatcher)
    val x = jobs.map { job =>
      if (job.key == key)
        job.key -> Seq(receiver)
      else
        job.key -> Seq.empty[ActorRef[StrippedPartitionFound]]
    }
    PendingJobMap.from(x)
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
