package com.github.codelionx.distod.actors.master

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.LeaderGuardian
import com.github.codelionx.distod.actors.master.Master.{Command, LocalPeers}
import com.github.codelionx.distod.actors.master.MasterHelper.{GenerateCandidates, InitWithAttributes, NextJob}
import com.github.codelionx.distod.actors.worker.Worker
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.partitions.StrippedPartition
import com.github.codelionx.distod.protocols.{PartitionManagementProtocol, ResultCollectionProtocol}
import com.github.codelionx.distod.protocols.DataLoadingProtocol._
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultCommand
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}
import com.github.codelionx.util.Math
import com.github.codelionx.util.largeMap.mutable.FastutilState
import com.github.codelionx.util.timing.Timing
import com.github.codelionx.util.GenericLogLevelLogger._


object Master {

  private[master] final val workerMessageMultiplier = 5

  sealed trait Command
  // only used from master helpers
  private[master] final case class DequeueNextJob(replyTo: ActorRef[Worker.Command]) extends Command
  private[master] final case class EnqueueCancelledJob(id: CandidateSet, jobType: JobType.JobType) extends Command
  private[master] final case class UpdateState(
      job: (CandidateSet, JobType.JobType),
      stateUpdates: Map[CandidateSet, Set[CandidateState.Delta]],
      prunedCandidates: Set[CandidateSet]
  ) extends Command
  private[master] final case class NewCandidatesGenerated(
      id: CandidateSet,
      jobType: JobType.JobType,
      newJobs: Set[(CandidateSet, JobType.JobType)],
      stateUpdates: Map[CandidateSet, Set[CandidateState.Delta]]
  ) extends Command

  // only used inside master
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command
  private final case class WrappedPartitionEvent(e: PartitionManagementProtocol.PartitionEvent) extends Command
  private case object StatisticsTick extends Command

  val MasterServiceKey: ServiceKey[MasterHelper.Command] = ServiceKey("master")

  val name = "master"

  def apply(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand],
      resultCollector: ActorRef[ResultCommand]
  ): Behavior[Command] = Behaviors.setup { context =>
    val settings = Settings(context.system)
    val stashSize = (
      scala.math.max(settings.maxWorkers, 10)
        * settings.concurrentWorkerJobs
        * workerMessageMultiplier
        * settings.expectedNodeCount
      )

    Behaviors.withStash(stashSize) { stash =>
      val masterBehavior =
        new Master(context, stash, LocalPeers(guardian, dataReader, partitionManager, resultCollector)).start()

      if (context.log.isEnabled(settings.monitoringSettings.statisticsLogLevel)) {
        Behaviors.withTimers { timers =>
          val interval = settings.monitoringSettings.statisticsLogInterval
          timers.startTimerWithFixedDelay("statistics-tick", StatisticsTick, interval)
          masterBehavior
        }
      } else {
        masterBehavior
      }
    }
  }

  case class LocalPeers(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand],
      resultCollector: ActorRef[ResultCommand]
  )
}


class Master(context: ActorContext[Command], stash: StashBuffer[Command], localPeers: LocalPeers)
  extends CandidateGeneration {

  import Master._
  import localPeers._


  private val state: FastutilState[CandidateState] = FastutilState.empty
  private val timing: Timing = Timing(context.system)
  private val timingSpans = timing.createSpans
  private var level: Int = 0

  private val settings = Settings(context.system)

  def start(): Behavior[Command] = initialize()

  private def initialize(): Behavior[Command] = Behaviors.setup { context =>
    // create helper pool and register it at the receptionist
    val pool = context.spawn(
      MasterHelper.createPool(settings.parallelism)(state, context.self, partitionManager),
      name = MasterHelper.poolName
    )
    context.system.receptionist ! Receptionist.Register(MasterServiceKey, pool)

    // load data
    val loadingEventMapper = context.messageAdapter(e => WrappedLoadingEvent(e))
    dataReader ! LoadPartitions(loadingEventMapper)

    Behaviors.receiveMessagePartial[Command] {
      case WrappedLoadingEvent(PartitionsLoaded(table @ PartitionedTable(name, headers, partitions))) =>
        context.log.info("Finished loading dataset {} with headers: {}", name, headers.mkString(","))

        // stop data reader to free up resources
        dataReader ! Stop

        val attributes = 0 until table.nAttributes
        val attributeSet = attributes.toSet
        partitionManager ! PartitionManagementProtocol.SetAttributes(attributes)
        resultCollector ! ResultCollectionProtocol.SetAttributeNames(headers.toIndexedSeq)
        pool ! InitWithAttributes(attributeSet)

        // L0: root candidate node
        val rootCandidateState = generateLevel0(attributeSet, table.nTuples)
        partitionManager ! InsertPartition(CandidateSet.empty, StrippedPartition(
          nTuples = table.nTuples,
          numberElements = table.nTuples,
          numberClasses = 1,
          equivClasses = Array(Array.range(0, table.nTuples))
        ))

        // L1: single attribute candidate nodes
        val (l1candidates, l1candidateState) = generateLevel1(attributes)
        l1candidates.zipWithIndex.foreach { case (candidate, index) =>
          partitionManager ! InsertPartition(candidate, partitions(index))
        }

        // L2: two attribute candidate nodes (initialized states)
        val L2candidateState = generateLevel2(attributeSet, l1candidates)

        state.addAll(rootCandidateState ++ l1candidateState ++ L2candidateState)

        val initialQueue = l1candidates.map(key => key -> JobType.Split)
        context.log.info("Master ready, initial work queue: {}", initialQueue)
        context.log.trace("Initial state:\n{}", state.mkString("\n"))
        stash.unstashAll(
          behavior(pool, attributeSet, WorkQueue.from(initialQueue), Set.empty, 0)
        )
    }
  }

  private def behavior(
      pool: ActorRef[MasterHelper.Command],
      attributes: Set[Int],
      workQueue: WorkQueue,
      pendingGenerationJobs: Set[(CandidateSet, JobType.JobType)],
      testedCandidates: Int
  ): Behavior[Command] = Behaviors.receiveMessage {
    case DequeueNextJob(_) if workQueue.isEmpty && pendingGenerationJobs.isEmpty =>
      context.log.debug("Request for work, but no more work available and no pending requests: algo finished!")
      finished(testedCandidates)

    case DequeueNextJob(replyTo) if workQueue.hasWork =>
      timingSpans.start("Master dispatch work")
      val (job, newWorkQueue) = workQueue.dequeue()
      val (id, _) = job
      pool ! NextJob(job, replyTo)
      if (context.log.isInfoEnabled && id.size > level) {
        context.log.info(
          "Entering next level {}, size = {}",
          id.size,
          Math.binomialCoefficient(attributes.size, id.size)
        )
        level = id.size
      }
      timingSpans.end("Master dispatch work")
      behavior(pool, attributes, newWorkQueue, pendingGenerationJobs, testedCandidates)

    case m: DequeueNextJob =>
      context.log.trace("Stashing request for work from {}", m.replyTo)
      stash.stash(m)
      Behaviors.same

    case EnqueueCancelledJob(id, jobType) =>
      val job = id -> jobType
      context.log.info("Re-enqueueing cancelled job {}", job)
      val newQueue = workQueue.removePending(job).enqueue(job)
      behavior(pool, attributes, newQueue, pendingGenerationJobs, testedCandidates)

    case UpdateState(job, stateUpdates, prunedCandidates) =>
      context.log.debug("Updating state for job {}: pruned candidates: {}", job, prunedCandidates.size)
      timingSpans.start("State integration")
      val (id, jobType) = job
      val updatedWorkQueue = workQueue
        .removePending(job)
        .removeAll(prunedCandidates)

      // update states
      stateUpdates.foreach { case (id, updates) =>
        state.updateWith(id) {
          case None => Some(CandidateState(id).updatedAll(updates))
          case Some(s) => Some(s.updatedAll(updates))
        }
      }
      // only generate next candidates if size limit is not reached
      if(settings.pruning.odSizeLimit.forall(limit => id.size < limit)) {
        // get successor states for candidate generation
        val successorStates = id.successors(attributes).map { successor =>
          state.getOrElse(successor, CandidateState(successor))
        }
        pool ! GenerateCandidates(id, jobType, successorStates)
        timingSpans.end("State integration")
        behavior(pool, attributes, updatedWorkQueue, pendingGenerationJobs + job, testedCandidates + 1)
      } else {
        timingSpans.end("State integration")
        stash.unstashAll(
          behavior(pool, attributes, updatedWorkQueue, pendingGenerationJobs, testedCandidates + 1)
        )
      }

    case NewCandidatesGenerated(id, jobType, newJobs, stateUpdates) =>
      timingSpans.start("State integration")
      val job = id -> jobType
      // add new jobs to the queue
      val updatedWorkQueue = workQueue.enqueueAll(newJobs)

      // add new candidates to successor states
      stateUpdates.foreach { case (id, updates) =>
        state.updateWith(id) {
          case None =>
            // should not happen
            throw new IllegalArgumentException(s"Tried to update non-existent state for $id")
          case Some(s) => Some(s.updatedAll(updates))
        }
      }
      timingSpans.end("State integration")
      stash.unstashAll(
        behavior(pool, attributes, updatedWorkQueue, pendingGenerationJobs - job, testedCandidates)
      )

    case StatisticsTick =>
      context.log.log(settings.monitoringSettings.statisticsLogLevel,
        "Master statistics: work / pending queue size=({} / {}), pending gen. jobs={}, stashed workers={}",
        workQueue.sizeWork,
        workQueue.sizePending,
        pendingGenerationJobs.size,
        stash.size
      )
      Behaviors.same

    case m =>
      context.log.warn("Received unexpected message: {}", m)
      Behaviors.same
  }

  private def finished(testedCandidates: Int): Behavior[Command] = {
    println(s"Tested candidates: $testedCandidates")
    guardian ! LeaderGuardian.AlgorithmFinished
    Behaviors.receiveMessage {
      case DequeueNextJob(_) =>
        // can be ignored without issues, because we are out of work
        Behaviors.same
      case StatisticsTick =>
        context.log.log(
          settings.monitoringSettings.statisticsLogLevel,
          "Master statistics (algorithm finished): stashed workers={}",
          stash.size
        )
        Behaviors.same
      case m =>
        context.log.warn("Ignoring message because we are in shutdown: {}", m)
        Behaviors.same
    }
  }

}
