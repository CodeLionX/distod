package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer, TimerScheduler}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.SystemMonitor
import com.github.codelionx.distod.actors.SystemMonitor.{CriticalHeapUsage, Register, SystemEvent}
import com.github.codelionx.distod.actors.partitionMgmt.PartitionGenerator.{ComputePartition, ComputePartitions}
import com.github.codelionx.distod.actors.partitionMgmt.channel.PartitionManagerEndpoint
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.largeMap.mutable.PendingJobMap
import com.github.codelionx.util.timing.Timing


object PartitionManager {

  private[partitionMgmt] case class ProductComputed(key: CandidateSet, partition: StrippedPartition)
    extends PartitionCommand
  private case object Cleanup extends PartitionCommand
  private final case class WrappedSystemEvent(event: SystemEvent) extends PartitionCommand
  private sealed trait PendingResponse
  private final case class PendingError(replyTo: ActorRef[ErrorFound], candidateId: CandidateSet)
    extends PendingResponse
  private final case class PendingStrippedPartition(
      replyTo: ActorRef[StrippedPartitionFound], candidateId: CandidateSet
  ) extends PendingResponse

  val name = "partition-manager"

  def apply(monitor: ActorRef[SystemMonitor.Command]): Behavior[PartitionCommand] = Behaviors.setup { context =>
    val settings = Settings(context.system)
    val stashSize = settings.numberOfWorkers * 4
    Behaviors.withStash(stashSize) { stash =>
      Behaviors.withTimers { timers =>
        new PartitionManager(context, stash, timers, monitor).start()
      }
    }
  }

}


class PartitionManager(
    context: ActorContext[PartitionCommand],
    stash: StashBuffer[PartitionCommand],
    timers: TimerScheduler[PartitionCommand],
    monitor: ActorRef[SystemMonitor.Command]
) {

  import PartitionManager._


  private val settings = Settings(context.system)
  private val compactionSettings = settings.partitionCompactionSettings
  private val generatorPool =
    if (settings.numberOfWorkers > 0)
      Some(context.spawn(
        PartitionGenerator.createPool(settings.numberOfWorkers),
        name = PartitionGenerator.poolName
      ))
    else
      None
  private val timings = Timing(context.system)

  private var partitions: CompactingPartitionMap = _
  private val pendingJobs: PendingJobMap[CandidateSet, PendingResponse] = PendingJobMap.empty

  def start(): Behavior[PartitionCommand] = {
    if (compactionSettings.enabled)
      timers.startTimerWithFixedDelay("cleanup", Cleanup, compactionSettings.interval)

    monitor ! Register(context.messageAdapter(WrappedSystemEvent))
    initialize(Seq.empty, Map.empty)
  }

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

    case Cleanup | WrappedSystemEvent(_) =>
      // ignore
      Behaviors.same

    case m =>
      context.log.warn("Stashing request {}", m.getClass.getSimpleName)
      stash.stash(m)
      Behaviors.same
  }

  private def nextBehavior(
      attributes: Seq[Int],
      singletonPartitions: Map[CandidateSet, FullPartition]
  ): Behavior[PartitionCommand] =
    if (attributes.nonEmpty && singletonPartitions.size == attributes.size) {
      context.log.info(
        "Initialization of partition manager finished, received {} attributes and {} partitions. {}",
        attributes.size,
        singletonPartitions.size,
        if (settings.directPartitionProductThreshold > 2)
          "Partition product of deep partitions is computed directly, when it would take more than " +
            s"${settings.directPartitionProductThreshold} steps to compute it incrementally."
        else
          "Partition product is always computed directly."
      )
      partitions = CompactingPartitionMap(compactionSettings).from(singletonPartitions)
      stash.unstashAll(
        behavior(attributes)
      )
    } else {
      initialize(attributes, singletonPartitions)
    }

  private def behavior(attributes: Seq[Int]): Behavior[PartitionCommand] = Behaviors.receiveMessage {
    // inserts
    case SetAttributes(ignored) =>
      context.log.warn("Attempt to change attributes to {} after initialization step, ignoring!", ignored)
      Behaviors.same

    case InsertPartition(key, _: FullPartition) if key.size == 1 =>
      context.log.warn("Attempt to change initial partitions (key.size == 1) after initialization step, ignoring!")
      Behaviors.same

    case InsertPartition(key, value: StrippedPartition) =>
      context.log.debug("Inserting partition for key {}", key)
      partitions + (key -> value)
      Behaviors.same

    // request attributes
    case LookupAttributes(replyTo) =>
      replyTo ! AttributesFound(attributes)
      Behaviors.same

    // request from a partition replicator
    case OpenConnection(replyTo) =>
      context.log.info("Opening sidechannel to {}", replyTo)
      context.spawn(PartitionManagerEndpoint(partitions, replyTo, attributes), PartitionManagerEndpoint.name())
      Behaviors.same

    // single-attr keys
    case LookupError(candidateId, key, replyTo) if key.size == 1 =>
      partitions.getSingletonPartition(key) match {
        case Some(p) =>
          replyTo ! ErrorFound(candidateId, key, p.stripped.error)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Full partition for key $key not found! Can not compute error.")
      }

    case LookupPartition(candidateId, key, replyTo) if key.size == 1 =>
      partitions.getSingletonPartition(key) match {
        case Some(p) =>
          replyTo ! PartitionFound(candidateId, key, p)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Full partition for key $key not found!")
      }

    case LookupStrippedPartition(candidateId, key, replyTo) if key.size == 1 =>
      partitions.getSingletonPartition(key) match {
        case Some(p) =>
          replyTo ! StrippedPartitionFound(candidateId, key, p.stripped)
          Behaviors.same
        case None =>
          throw new IllegalAccessException(s"Stripped partition for key $key not found!")
      }

    // rest
    case LookupError(candidateId, key, replyTo) if key.size != 1 =>
      partitions.get(key) match {
        case Some(p: StrippedPartition) =>
          replyTo ! ErrorFound(candidateId, key, p.error)
          Behaviors.same
        case None if pendingJobs.contains(key) =>
          context.log.trace("Error is being computed, queuing request for key {}", key)
          pendingJobs + (key -> PendingError(replyTo, candidateId))
          Behaviors.same
        case None =>
          context.log.debug("Partition to compute error not found. Starting background job to compute {}", key)
          generateStrippedPartitionJobs(key, PendingError(replyTo, candidateId))
          Behaviors.same
      }

    case LookupPartition(_, key, _) if key.size != 1 =>
      throw new IllegalArgumentException(
        s"Only keys of size 1 contain full partitions, but your key has ${key.size} elements!"
      )

    case LookupStrippedPartition(candidateId, key, replyTo) if key.size != 1 =>
      partitions.get(key) match {
        case Some(p: StrippedPartition) =>
          replyTo ! StrippedPartitionFound(candidateId, key, p)
          Behaviors.same
        case None if pendingJobs.contains(key) =>
          context.log.trace("Partition is being computed, queuing request for key {}", key)
          pendingJobs + (key -> PendingStrippedPartition(replyTo, candidateId))
          Behaviors.same
        case None =>
          context.log.debug("Partition not found. Starting background job to compute {}", key)
          generateStrippedPartitionJobs(key, PendingStrippedPartition(replyTo, candidateId))
          Behaviors.same
      }

    case ProductComputed(key, partition) =>
      context.log.trace("Received computed partition for key {}", key)
      pendingJobs.get(key) match {
        case Some(pendingResponses) => pendingResponses.foreach {
          case PendingStrippedPartition(ref, candidateId) =>
            ref ! StrippedPartitionFound(candidateId, key, partition)
          case PendingError(ref, candidateId) =>
            ref ! ErrorFound(candidateId, key, partition.error)
        }
        case None => // do nothing
      }
      if (settings.cacheEnabled) partitions + (key -> partition)
      pendingJobs.removeKey(key)
      Behaviors.same

    case Cleanup =>
      partitions.compact()
      Behaviors.same

    case WrappedSystemEvent(CriticalHeapUsage) if settings.cacheEnabled =>
      context.log.warn("Clearing all temporary partitions in consequence of memory shortage")
      partitions.clear()
      Behaviors.same

    case WrappedSystemEvent(CriticalHeapUsage) if settings.cacheDisabled =>
      // ignore
      Behaviors.same
  }

  private def generateStrippedPartitionJobs(key: CandidateSet, pendingResponse: PendingResponse): Unit = {
    timings.time("Partition generation") {
      generatorPool match {
        case Some(pool) =>
          lazy val jobs = JobChainer.calcJobChain(key, partitions)
          val poolJob = settings.directPartitionProductThreshold match {
            case 0 | 1 =>
              directProductJob(key, pendingResponse)
            case theta if jobs.size >= theta =>
              context.log.trace("Avoiding generation of expensive job chain of size {} " +
                "by generating partition for {} directly from singleton partitions", jobs.size, key)
              directProductJob(key, pendingResponse)
            case _ =>
              incrementalProductJob(key, pendingResponse, jobs)
          }
          pool ! poolJob
        case None =>
          context.log.error("Could not generate partition, because the partition generator pool is not available (max-workers < 1)")
      }
    }
  }

  private def directProductJob(key: CandidateSet, pendingResponse: PendingResponse): PartitionGenerator.Command = {
    pendingJobs + (key, pendingResponse)
    ComputePartition.createFrom(partitions)(key, context.self)
  }

  private def incrementalProductJob(
      key: CandidateSet,
      pendingResponse: PendingResponse,
      jobs: Seq[ComputePartitionProductJob]
  ): PartitionGenerator.Command = {
    val additionalPending = jobs
      .filter(_.store)
      .map {
        case job if job.key == key =>
          job.key -> Seq(pendingResponse)
        case job =>
          job.key -> Seq.empty
      }
    pendingJobs ++ additionalPending
    ComputePartitions(jobs, context.self)
  }
}
