package com.github.codelionx.distod.actors.master

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.LeaderGuardian
import com.github.codelionx.distod.actors.master.Master.{Command, LocalPeers}
import com.github.codelionx.distod.actors.master.MasterHelper.{DispatchWorkTo, GenerateCandidates}
import com.github.codelionx.distod.actors.partitionMgmt.PartitionReplicator.PrimaryPartitionManager
import com.github.codelionx.distod.actors.worker.Worker
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates}
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.partitions.StrippedPartition
import com.github.codelionx.distod.protocols.DataLoadingProtocol._
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultCommand
import com.github.codelionx.distod.protocols.{PartitionManagementProtocol, ResultCollectionProtocol}
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}
import com.github.codelionx.util.Math
import com.github.codelionx.util.largeMap.mutable.FastutilState
import com.github.codelionx.util.timing.Timing


object Master {

  sealed trait Command
  final case class DispatchWork(replyTo: ActorRef[Worker.Command]) extends Command with CborSerializable
  final case class SplitCandidatesChecked(id: CandidateSet, removedSplitCandidates: CandidateSet)
    extends Command with CborSerializable
  final case class SwapCandidatesChecked(id: CandidateSet, removedSwapCandidates: Seq[(Int, Int)])
    extends Command with CborSerializable
  final case class GetPrimaryPartitionManager(replyTo: ActorRef[PrimaryPartitionManager])
    extends Command with CborSerializable
  final case class UpdateState(
      job: (CandidateSet, JobType.JobType),
      stateUpdates: Map[CandidateSet, Set[CandidateState.Delta]],
      prunedCandidates: Set[CandidateSet]
  ) extends Command
  final case class NewCandidatesGenerated(
      id: CandidateSet,
      jobType: JobType.JobType,
      newJobs: Set[(CandidateSet, JobType.JobType)],
      stateUpdates: Map[CandidateSet, Set[CandidateState.Delta]]
  ) extends Command
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command
  private final case class WrappedPartitionEvent(e: PartitionManagementProtocol.PartitionEvent) extends Command

  val MasterServiceKey: ServiceKey[Command] = ServiceKey("master")

  val name = "master"

  def apply(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand],
      resultCollector: ActorRef[ResultCommand]
  ): Behavior[Command] = Behaviors.setup(context =>
    Behaviors.withStash(300) { stash =>
      new Master(context, stash, LocalPeers(guardian, dataReader, partitionManager, resultCollector)).start()
    })

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
  private val timingSpans = timing.spans
  private var level: Int = 0

  private val settings = Settings(context.system)
  private val pool = context.spawn(
    MasterHelper.createPool(settings.numberOfWorkers)(state, context.self),
    name = MasterHelper.poolName
  )

  def start(): Behavior[Command] = initialize()

  private def initialize(): Behavior[Command] = Behaviors.setup { context =>
    // register message adapters
    val loadingEventMapper = context.messageAdapter(e => WrappedLoadingEvent(e))

    // make master available to the whole cluster using the registry
    context.system.receptionist ! Receptionist.Register(MasterServiceKey, context.self)

    dataReader ! LoadPartitions(loadingEventMapper)

    Behaviors.receiveMessagePartial[Command] {
      case m: DispatchWork =>
        context.log.debug("Worker {} is ready for work, stashing request", m.replyTo)
        stash.stash(m)
        Behaviors.same

      case GetPrimaryPartitionManager(replyTo) =>
        replyTo ! PrimaryPartitionManager(partitionManager)
        Behaviors.same

      case WrappedLoadingEvent(PartitionsLoaded(table @ PartitionedTable(name, headers, partitions))) =>
        context.log.info("Finished loading dataset {} with headers: {}", name, headers.mkString(","))

        // stop data reader to free up resources
        dataReader ! Stop

        val attributes = 0 until table.nAttributes
        partitionManager ! PartitionManagementProtocol.SetAttributes(attributes)
        resultCollector ! ResultCollectionProtocol.SetAttributeNames(headers.toIndexedSeq)

        // L0: root candidate node
        val rootCandidateState = generateLevel0(attributes, table.nTuples)
        partitionManager ! InsertPartition(CandidateSet.empty, StrippedPartition(
          nTuples = table.nTuples,
          numberElements = table.nTuples,
          numberClasses = 1,
          equivClasses = IndexedSeq((0 until table.nTuples).toSet)
        ))

        // L1: single attribute candidate nodes
        val (l1candidates, l1candidateState) = generateLevel1(attributes, partitions)
        l1candidates.zipWithIndex.foreach { case (candidate, index) =>
          partitionManager ! InsertPartition(candidate, partitions(index))
        }

        // L2: two attribute candidate nodes (initialized states)
        val L2candidateState = generateLevel2(attributes, l1candidates)

        // first reshape and then add elements to prevent copy operation
        state.reshapeMaps(attributes.size)
        state.addAll(rootCandidateState ++ l1candidateState ++ L2candidateState)

        val initialQueue = l1candidates.map(key => key -> JobType.Split)
        context.log.info("Master ready, initial work queue: {}", initialQueue)
        context.log.trace("Initial state:\n{}", state.mkString("\n"))
        stash.unstashAll(
          behavior(attributes, WorkQueue.from(initialQueue), Set.empty, 0)
        )
    }
  }

  private def behavior(
      attributes: Seq[Int],
      workQueue: WorkQueue,
      pendingGenerationJobs: Set[(CandidateSet, JobType.JobType)],
      testedCandidates: Int
  ): Behavior[Command] = Behaviors.receiveMessage {
    case GetPrimaryPartitionManager(replyTo) =>
      replyTo ! PrimaryPartitionManager(partitionManager)
      Behaviors.same

    case DispatchWork(_) if workQueue.isEmpty && pendingGenerationJobs.isEmpty =>
      context.log.debug("Request for work, but no more work available and no pending requests: algo finished!")
      finished(testedCandidates)

    case DispatchWork(worker) if workQueue.hasWork =>
      timingSpans.start("Master dispatch work")
      val ((id, jobType), newWorkQueue) = workQueue.dequeue()
      pool ! DispatchWorkTo(id, jobType, worker)
      if(context.log.isInfoEnabled && id.size > level) {
        context.log.info(
          "Entering next level {}, size = {}",
          id.size,
          Math.binomialCoefficient(attributes.size, id.size)
        )
        level = id.size
      }
      behavior(attributes, newWorkQueue, pendingGenerationJobs, testedCandidates)

    case m: DispatchWork =>
      context.log.trace("Stashing request for work from {}", m.replyTo)
      stash.stash(m)
      Behaviors.same

    case SplitCandidatesChecked(id, removedSplitCandidates) =>
      pool ! MasterHelper.SplitCandidatesChecked(id, attributes.toSet, removedSplitCandidates)
      behavior(attributes, workQueue, pendingGenerationJobs, testedCandidates)

    case SwapCandidatesChecked(id, removedSwapCandidates) =>
      pool ! MasterHelper.SwapCandidatesChecked(id, attributes.toSet, removedSwapCandidates)
      behavior(attributes, workQueue, pendingGenerationJobs, testedCandidates)

    case UpdateState(job, stateUpdates, prunedCandidates) =>
      context.log.debug("Updating state for job {}: pruned candidates: {}", job, prunedCandidates.size)
      timingSpans.start("State integration")
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
      timingSpans.end("State integration")
      // get successor states
      val successorStates = job._1.successors(attributes.toSet).map { successor =>
        state.getOrElse(successor, CandidateState(successor))
      }
      pool ! GenerateCandidates(job._1, job._2, successorStates)
      behavior(attributes, updatedWorkQueue, pendingGenerationJobs + job, testedCandidates + 1)

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
        behavior(attributes, updatedWorkQueue, pendingGenerationJobs - job, testedCandidates)
      )

    case m =>
      context.log.warn("Received unexpected message: {}", m)
      Behaviors.same
  }

  private def finished(testedCandidates: Int): Behavior[Command] = {
    println(s"Tested candidates: $testedCandidates")
    guardian ! LeaderGuardian.AlgorithmFinished
    Behaviors.receiveMessage {
      case DispatchWork(_) =>
        // can be ignored without issues, because we are out of work
        Behaviors.same
      case m =>
        context.log.warn("Ignoring message because we are in shutdown: {}", m)
        Behaviors.same
    }
  }

}
