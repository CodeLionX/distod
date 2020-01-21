package com.github.codelionx.distod.actors.master

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.LeaderGuardian
import com.github.codelionx.distod.actors.master.Master.{Command, LocalPeers}
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
          behavior(attributes, WorkQueue.from(initialQueue), 0)
        )
    }
  }

  private def behavior(
      attributes: Seq[Int],
      workQueue: WorkQueue,
      testedCandidates: Int
  ): Behavior[Command] = Behaviors.receiveMessage {
    case GetPrimaryPartitionManager(replyTo) =>
      replyTo ! PrimaryPartitionManager(partitionManager)
      Behaviors.same

    case DispatchWork(_) if workQueue.isEmpty =>
      context.log.trace("Request for work, but no more work available and no pending requests: algo finished!")
      finished(testedCandidates)

    case m: DispatchWork if workQueue.noWork && workQueue.hasPending =>
      context.log.trace("Stashing request for work from {}", m.replyTo)
      stash.stash(m)
      Behaviors.same

    case DispatchWork(replyTo) if workQueue.hasWork =>
      val newWorkQueue = timing.unsafeTime("Dequeueing") {
        val ((taskId, jobType), newWorkQueue) = workQueue.dequeue()
        val taskState = state(taskId)
        if(context.log.isInfoEnabled && taskId.size > level) {
          context.log.info(
            "Entering next level {}, size = {}",
            taskId.size,
            Math.binomialCoefficient(attributes.size, taskId.size)
          )
          level = taskId.size
        }
        context.log.debug("Dispatching task {} to {}", taskId -> jobType, replyTo)
        jobType match {
          case JobType.Split =>
            val splitCandidates = taskId & taskState.splitCandidates
            replyTo ! CheckSplitCandidates(taskId, splitCandidates)
          case JobType.Swap =>
            val swapCandidates = taskState.swapCandidates
            replyTo ! CheckSwapCandidates(taskId, swapCandidates)
        }
        newWorkQueue
      }
      behavior(attributes, newWorkQueue, testedCandidates)

    case SplitCandidatesChecked(id, removedSplitCandidates) =>
      val job = id -> JobType.Split
      val stateUpdate = CandidateState.SplitChecked(removedSplitCandidates)
      updateStateAndNext(attributes, workQueue, testedCandidates, job, stateUpdate)

    case SwapCandidatesChecked(id, removedSwapCandidates) =>
      val job = id -> JobType.Swap
      val stateUpdate = CandidateState.SwapChecked(removedSwapCandidates)
      updateStateAndNext(attributes, workQueue, testedCandidates, job, stateUpdate)

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

  private def updateStateAndNext(
      attributes: Seq[Int],
      workQueue: WorkQueue,
      testedCandidates: Int,
      job: (CandidateSet, JobType.JobType),
      stateUpdate: CandidateState.Delta
  ): Behavior[Command] = {
    context.log.debug("Received results for {}", job)
    val (id, jobType) = job

    timingSpans.start("Common state update")
    // update state based on received results
    val newWorkQueue = workQueue.removePending(job)
    val newTaskState = state.updateWith(id) {
      case None => None
      case Some(s) => Some(s.updated(stateUpdate).pruneIfConditionsAreMet)
    }

    // update successors and get new generation jobs
    val successors = id.successors(attributes.toSet)
    val nodeIsPruned = newTaskState.forall(_.isPruned)
    timingSpans.end("Common state update")

    if (nodeIsPruned) {
      timingSpans.start("State pruning")
      context.log.debug("Pruning node {} and all successors", id)
      // node pruning! --> invalidate all successing nodes
      successors.foreach(s =>
        state.updateWith(s) {
          case Some(value) => Some(value.prune)
          case None => Some(CandidateState.pruned(s))
        }
      )
      // remove all jobs that involve one of the pruned successors
      val updatedWorkQueue = newWorkQueue.removeAll(successors)
      timingSpans.end("State pruning")
      behavior(attributes, updatedWorkQueue, testedCandidates + 1)
    } else {
      timingSpans.start("Common state update")
      // update counters of successors
      val successorStates = successors.map { successor =>
        val oldState = state.getOrElse(successor, CandidateState(successor))
        val successorState = oldState.incPreconditions(jobType)
        state.update(successor, successorState)
        successorState
      }
      timingSpans.end("Common state update")

      timingSpans.start("Candidate generation")
      // generate successor's splits candidates
      val splitReadySuccessors = successorStates.filter(successorState =>
        // only check split readiness if we changed the split preconditions (otherwise swap updates would also trigger
        // the new generation of split candidates)
        if (jobType == JobType.Split) successorState.isReadyToCheck(JobType.Split)
        else false
      )
      val newSplitJobs = splitReadySuccessors.map(s => s.id -> JobType.Split)
      val splitStateUpdates = splitReadySuccessors.map(performSplitGeneration)

      // generate successor's swaps candidates
      val swapReadySuccessors = successorStates.filter(successorState =>
        successorState.isReadyToCheck(JobType.Swap)
      )
      val newSwapJobs = swapReadySuccessors.map(s => s.id -> JobType.Swap)
      val swapStateUpdates = swapReadySuccessors.map(performSwapGeneration)

      // group state updates
      val stateUpdates = (splitStateUpdates ++ swapStateUpdates)
        .groupBy { case (id, _) => id }
        .map { case (key, value) => key -> value.map(_._2) }
      timingSpans.end("Candidate generation")

      timingSpans.start("Common state update")
      // add new jobs to the queue
      val updatedWorkQueue = newWorkQueue.enqueueAll(newSplitJobs ++ newSwapJobs)

      // add new candidates to successor states
      stateUpdates.foreach { case (id, updates) =>
        state.updateWith(id) {
          case None =>
            // Some(CandidateState.createFromDeltas(id, updates))
            // should not happen
            throw new IllegalArgumentException(s"Tried to update non-existent state for $id")
          case Some(s) => Some(s.updatedAll(updates))
        }
      }
      timingSpans.end("Common state update")
      behavior(attributes, updatedWorkQueue, testedCandidates + 1)
    }
  }

  private def performSplitGeneration(candidateState: CandidateState): (CandidateSet, CandidateState.Delta) = {
    context.log.trace("Generating split candidates for node {}", candidateState.id)
    val candidates = generateSplitCandidates(candidateState.id, state.view)
    candidateState.id -> CandidateState.NewSplitCandidates(candidates)
  }

  private def performSwapGeneration(candidateState: CandidateState): (CandidateSet, CandidateState.Delta) = {
    context.log.trace("Generating swap candidates for node {}", candidateState.id)
    val candidates = generateSwapCandidates(candidateState.id, state.view)
    candidateState.id -> CandidateState.NewSwapCandidates(candidates)
  }

}
