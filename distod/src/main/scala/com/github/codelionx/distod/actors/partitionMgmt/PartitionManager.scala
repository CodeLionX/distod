package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.partitionMgmt.PartitionGenerator.ComputePartitions
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PendingJobMap}
import com.github.codelionx.util.timing.Timing

import scala.concurrent.duration._
import scala.language.postfixOps


object PartitionManager {

  private[partitionMgmt] case class ProductComputed(key: CandidateSet, partition: StrippedPartition)
    extends PartitionCommand
  private case object Cleanup extends PartitionCommand

  private sealed trait PendingResponse
  private final case class PendingError(replyTo: ActorRef[ErrorFound]) extends PendingResponse
  private final case class PendingStrippedPartition(replyTo: ActorRef[StrippedPartitionFound]) extends PendingResponse

  val name = "partition-manager"

  def apply(): Behavior[PartitionCommand] = Behaviors.setup(context =>
    Behaviors.withStash(300) { stash =>
      Behaviors.withTimers{ timers =>
        new PartitionManager(context, stash, timers).start()
  }})

}


class PartitionManager(context: ActorContext[PartitionCommand], stash: StashBuffer[PartitionCommand], timers: TimerScheduler[PartitionCommand]) {

  import PartitionManager._


  def start(): Behavior[PartitionCommand] = {
    timers.startTimerWithFixedDelay("cleanup", Cleanup, 5 seconds)
    initialize(Seq.empty, Map.empty)
  }

  private val settings = Settings(context.system)

  private val generatorPool = context.spawn(
    PartitionGenerator.createPool(settings.numberOfWorkers),
    name = PartitionGenerator.poolName,
    // FIXME: only creates the head actor with the props (instead of the pool workers)
//    props = settings.cpuBoundTaskDispatcher
  )

  private val timings = Timing(context.system)

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

    case Cleanup =>
      // ignore
      Behaviors.same

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
        behavior(attributes, PartitionMap.from(singletonPartitions), PendingJobMap.empty, Int.MaxValue, 2)
      )
    } else {
      initialize(attributes, singletonPartitions)
    }

  private def behavior(
      attributes: Seq[Int],
      partitions: PartitionMap,
      pendingJobs: PendingJobMap[CandidateSet, PendingResponse],
      minLevel: Int,
      currentLevel: Int
  ): Behavior[PartitionCommand] = {
    def next(
              _attributes: Seq[Int] = attributes,
              _partitions: PartitionMap = partitions,
              _pendingJobs: PendingJobMap[CandidateSet, PendingResponse] = pendingJobs,
              _minLevel: Int = minLevel,
              _currentLevel: Int = currentLevel
            ): Behavior[PartitionCommand] =
      behavior(_attributes, _partitions, _pendingJobs, _minLevel, _currentLevel)

    Behaviors.receiveMessage {

      // inserts
      case SetAttributes(ignored) =>
        context.log.warn("Attempt to change attributes to {} after initialization step, ignoring!", ignored)
        Behaviors.same

      case InsertPartition(key, _: FullPartition) if key.size == 1 =>
        context.log.warn("Attempt to change initial partitions (key.size == 1) after initialization step, ignoring!")
        Behaviors.same

      case InsertPartition(key, value: StrippedPartition) =>
        context.log.debug("Inserting partition for key {}", key)
        next(_partitions = partitions + (key -> value))

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
        if(key.size > 2 && key.size < currentLevel) {
          context.log.warn("Accessing partition that was previously removed: {} (current level: {})", key, currentLevel)
        }
        partitions.get(key) match {
          case Some(p: StrippedPartition) =>
            replyTo ! ErrorFound(key, p.error)
            next(_minLevel = math.min(minLevel, key.size))
          case None if pendingJobs.contains(key) =>
            context.log.trace("Error is being computed, queuing request for key {}", key)
            next(_pendingJobs = pendingJobs + (key -> PendingError(replyTo)), _minLevel = math.min(minLevel, key.size))
          case None =>
            context.log.debug("Partition to compute error not found. Starting background job to compute {}", key)
            val newJobs = generateStrippedPartitionJobs(
              key,
              partitions,
              pendingJobs,
              PendingError(replyTo)
            )
            next(_pendingJobs = newJobs, _minLevel = math.min(minLevel, key.size))
        }

      case LookupPartition(key, _) if key.size != 1 =>
        throw new IllegalArgumentException(
          s"Only keys of size 1 contain full partitions, but your key has ${key.size} elements!"
        )

      case LookupStrippedPartition(key, replyTo) if key.size != 1 =>
        if(key.size > 2 && key.size < currentLevel) {
          context.log.warn("Accessing partition that was previously removed: {} (current level: {})", key, currentLevel)
        }
        partitions.get(key) match {
          case Some(p: StrippedPartition) =>
            replyTo ! StrippedPartitionFound(key, p)
            next(_minLevel = math.min(minLevel, key.size))
          case None if pendingJobs.contains(key) =>
            context.log.trace("Partition is being computed, queuing request for key {}", key)
            next(_pendingJobs = pendingJobs + (key -> PendingStrippedPartition(replyTo)), _minLevel = math.min(minLevel, key.size))
          case None =>
            context.log.debug("Partition not found. Starting background job to compute {}", key)
            val newJobs = generateStrippedPartitionJobs(
              key,
              partitions,
              pendingJobs,
              PendingStrippedPartition(replyTo)
            )
            next(_pendingJobs = newJobs, _minLevel = math.min(minLevel, key.size))
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
        next(_partitions = updatedPartitions, _pendingJobs = pendingJobs.keyRemoved(key))

      case Cleanup if minLevel >= currentLevel =>
        val updatedPartitions = (currentLevel until minLevel).foldLeft(partitions){ case (parts, level) =>
          parts.removeLevel(level)
        }
        if(context.log.isInfoEnabled) {
          val removed = partitions.size - updatedPartitions.size
          if(removed > 0) {
            context.log.info("Cleaning up {} partitions (< {})", removed, minLevel)
          }
          context.log.debug("Cleaning up until level {}: removed {} partitions", minLevel, removed)
        }
        next(_partitions = updatedPartitions, _minLevel = Int.MaxValue, _currentLevel = minLevel)

      case Cleanup => // skip
        context.log.debug("Skipping cleanup, current level: {}, min level: {}", currentLevel, minLevel)
        next(_minLevel = Int.MaxValue)
    }
  }

  private def generateStrippedPartitionJobs(
      key: CandidateSet,
      partitions: PartitionMap,
      pendingJobs: PendingJobMap[CandidateSet, PendingResponse],
      pendingResponse: PendingResponse
  ): PendingJobMap[CandidateSet, PendingResponse] = {
    timings.time("Partition generation") {
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
