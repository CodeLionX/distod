package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.partitionMgmt.PartitionGenerator.ComputePartitions
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PendingJobMap}


object PartitionManager {

  private[partitionMgmt] case class ProductComputed(key: CandidateSet, partition: StrippedPartition)
    extends PartitionCommand

  private sealed trait PendingResponse
  private final case class PendingError(replyTo: ActorRef[ErrorFound]) extends PendingResponse
  private final case class PendingStrippedPartition(replyTo: ActorRef[StrippedPartitionFound]) extends PendingResponse

  val name = "partition-manager"

  def apply(): Behavior[PartitionCommand] = Behaviors.setup(context => Behaviors.withStash(300) { stash =>
    new PartitionManager(context, stash).start()
  })

}


class PartitionManager(context: ActorContext[PartitionCommand], stash: StashBuffer[PartitionCommand]) {

  import PartitionManager._


  def start(): Behavior[PartitionCommand] = initialize(Seq.empty, Map.empty)

  private val settings = Settings(context.system)

  private val generatorPool = context.spawn(
    PartitionGenerator.createPool(settings.numberOfWorkers),
    name = PartitionGenerator.poolName,
    // FIXME: only creates the head actor with the props (instead of the pool workers)
    props = settings.cpuBoundTaskDispatcher
  )

  private def initialize(
      attributes: Seq[Int], singletonPartitions: Map[CandidateSet, FullPartition]
  ): Behavior[PartitionCommand] = Behaviors.receiveMessage {

    case SetAttributes(newAttributes) =>
      context.log.debug("Received attributes: {}", newAttributes)
      nextBehavior(newAttributes, singletonPartitions)

    case InsertPartition(key, value: FullPartition) if key.size == 1 =>
      context.log.debug("Inserting full partition for key {}", key)
      val newSingletonPartitions = singletonPartitions + (key -> value)
      nextBehavior(attributes, newSingletonPartitions)

    case m =>
      context.log.debug("Stashing request {}", m.getClass.getSimpleName)
      stash.stash(m)
      Behaviors.same
  }

  private def nextBehavior(
      attributes: Seq[Int], singletonPartitions: Map[CandidateSet, FullPartition]
  ): Behavior[PartitionCommand] =
    if (attributes.nonEmpty && singletonPartitions.size == attributes.size) {
      context.log.info(
        "Initialization of partition manager finished, received {} attributes and {} partitions",
        attributes.size,
        singletonPartitions.size
      )
      stash.unstashAll(
        behavior(attributes, PartitionMap.from(singletonPartitions), PendingJobMap.empty)
      )
    } else {
      initialize(attributes, singletonPartitions)
    }

  private def behavior(
      attributes: Seq[Int],
      partitions: PartitionMap,
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
      context.log.debug("Inserting partition for key {}", key)
      behavior(attributes, partitions + (key -> value), pendingJobs)

    // request attributes
    case LookupAttributes(replyTo) =>
      replyTo ! AttributesFound(attributes)
      Behaviors.same

    // single-attr keys
    case LookupError(key, replyTo) if key.size == 1 =>
      partitions.getSingletonPartition(key) match {
        case Some(p) =>
          replyTo ! ErrorFound(key, p.stripped.error)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Full partition for key ${key} not found! Can not compute error.")
      }

    case LookupPartition(key, replyTo) if key.size == 1 =>
      partitions.getSingletonPartition(key) match {
        case Some(p) =>
          replyTo ! PartitionFound(key, p)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Full partition for key ${key} not found!")
      }

    case LookupStrippedPartition(key, replyTo) if key.size == 1 =>
      partitions.getSingletonPartition(key) match {
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
          context.log.trace("Error is being computed, queuing request for key {}", key)
          behavior(attributes, partitions, pendingJobs + (key -> PendingError(replyTo)))
        case None =>
          context.log.debug("Partition to compute error not found. Starting background job to compute {}", key)
          val newJobs = generateStrippedPartitionJobs(
            key,
            partitions,
            pendingJobs,
            PendingError(replyTo)
          )
          behavior(attributes, partitions, newJobs)
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
          context.log.trace("Partition is being computed, queuing request for key {}", key)
          behavior(attributes, partitions, pendingJobs + (key -> PendingStrippedPartition(replyTo)))
        case None =>
          context.log.debug("Partition not found. Starting background job to compute {}", key)
          val newJobs = generateStrippedPartitionJobs(
            key,
            partitions,
            pendingJobs,
            PendingStrippedPartition(replyTo)
          )
          behavior(attributes, partitions, newJobs)
      }

    case ProductComputed(key, partition) =>
      context.log.trace("Received computed partition for key {}", key)
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
      behavior(attributes, updatedPartitions, pendingJobs.keyRemoved(key))
  }

  private def generateStrippedPartitionJobs(
      key: CandidateSet,
      partitions: PartitionMap,
      pendingJobs: PendingJobMap[CandidateSet, PendingResponse],
      pendingResponse: PendingResponse
  ): PendingJobMap[CandidateSet, PendingResponse] = {
    val jobs = calcJobChain(key, partitions)
    generatorPool ! ComputePartitions(jobs, context.self)
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
      partitions: PartitionMap
  ): Seq[ComputePartitionProductJob] = {

    def loop(subkey: CandidateSet): Seq[ComputePartitionProductJob] = {
      val predecessorKeys = subkey.predecessors.toSeq
      val foundKeys = predecessorKeys.filter(partitions.contains)
      val missingKeys = predecessorKeys.diff(foundKeys)

      foundKeys match {
        case Nil =>
          val nextPred1 :: nextPred2 :: _ = missingKeys
          loop(nextPred1) ++ loop(nextPred2) :+ ComputePartitionProductJob(subkey, Left(nextPred1), Left(nextPred2))

        case foundPred :: Nil =>
          val nextPred :: _ = missingKeys
          loop(nextPred) :+ ComputePartitionProductJob(subkey, Right(partitions(foundPred)), Left(nextPred))

        case _ =>
          val p1 :: p2 :: _ = foundKeys.map(partitions.apply).take(2)
          Seq(ComputePartitionProductJob(subkey, Right(p1), Right(p2)))
      }
    }

    loop(key)
  }
}
