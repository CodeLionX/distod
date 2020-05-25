package com.github.codelionx.distod.actors.master

import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, PoolRouter, Routers}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.CandidateState.{IncPrecondition, Prune}
import com.github.codelionx.distod.actors.master.Master.{DequeueNextJob, EnqueueCancelledJob, NewCandidatesGenerated, UpdateState}
import com.github.codelionx.distod.actors.partitionMgmt.PartitionReplicator.PrimaryPartitionManager
import com.github.codelionx.distod.actors.worker.Worker
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates}
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.Settings
import com.github.codelionx.util.largeMap.immutable.PendingJobMap
import com.github.codelionx.util.largeMap.mutable.FastutilState
import com.github.codelionx.util.timing.Timing

import scala.concurrent.duration._
import scala.language.postfixOps


object MasterHelper {

  sealed trait Command
  final case class DispatchWork(replyTo: ActorRef[Worker.Command]) extends Command with CborSerializable
  final case class CancelWork(id: CandidateSet, jobType: JobType.JobType) extends Command with CborSerializable
  final case class SplitCandidatesChecked(id: CandidateSet, removedSplitCandidates: CandidateSet)
    extends Command with CborSerializable
  final case class SwapCandidatesChecked(id: CandidateSet, removedSwapCandidates: Seq[(Int, Int)])
    extends Command with CborSerializable
  final case class GetPrimaryPartitionManager(replyTo: ActorRef[PrimaryPartitionManager])
    extends Command with CborSerializable

  private[master] final case class NextJob(job: (CandidateSet, JobType.JobType), replyTo: ActorRef[Worker.Command])
    extends Command
  private[master] final case class GenerateCandidates(
      id: CandidateSet, jobType: JobType.JobType, successorStates: Set[CandidateState]
  ) extends Command
  private[master] final case class InitWithAttributes(attributes: Set[Int]) extends Command

  val poolName = "master-helper-pool"

  def name(n: Int): String = s"master-helper-$n"

  def createPool(n: Int)(
      state: FastutilState[CandidateState],
      master: ActorRef[Master.Command],
      partitionManager: ActorRef[PartitionCommand]
  ): Behavior[Command] = Behaviors.setup { context =>
    val settings = Settings(context.system)
    val stashSize = (
      scala.math.max(settings.maxWorkers, 10)
        * settings.concurrentWorkerJobs
        * Master.workerMessageMultiplier
        * settings.expectedNodeCount
      )

    // lazy initialization of master helper pool to make attributes available for all routees during creation
    Behaviors.withStash(stashSize)(stash =>
      Behaviors.receiveMessage {
        case InitWithAttributes(attributes) =>
          stash.unstashAll(
            createConfiguredPool(n, state, master, partitionManager, attributes)
          )
        case m =>
          stash.stash(m)
          Behaviors.same
      }
    )
  }

  private def createConfiguredPool(
      n: Int,
      state: FastutilState[CandidateState],
      master: ActorRef[Master.Command],
      partitionManager: ActorRef[PartitionCommand],
      attributes: Set[Int]
  ): PoolRouter[Command] = Routers.pool(n)(
    Behaviors.supervise(apply(state, master, partitionManager, attributes)).onFailure[Exception](
      SupervisorStrategy.restart
        .withLoggingEnabled(true)
        .withLimit(3, 10 seconds)
    )
  ).withRoundRobinRouting()

  private def apply(
      state: FastutilState[CandidateState],
      master: ActorRef[Master.Command],
      partitionManager: ActorRef[PartitionCommand],
      attributes: Set[Int]
  ): Behavior[Command] = Behaviors.setup(context =>
    new MasterHelper(context, state, master, partitionManager, attributes).start()
  )
}

class MasterHelper(
    context: ActorContext[MasterHelper.Command],
    state: FastutilState[CandidateState],
    master: ActorRef[Master.Command],
    partitionManager: ActorRef[PartitionCommand],
    attributes: Set[Int]
) extends CandidateGeneration {

  import MasterHelper._


  private val timingSpans = Timing(context.system).createSpans

  def start(): Behavior[Command] = Behaviors.receiveMessage {
    case GetPrimaryPartitionManager(replyTo) =>
      replyTo ! PrimaryPartitionManager(partitionManager)
      Behaviors.same

    case DispatchWork(replyTo) =>
      master ! DequeueNextJob(replyTo)
      Behaviors.same

    case NextJob(job, replyTo) =>
      timingSpans.start("Helper dispatch work")
      val (id, jobType) = job
      val taskState = state(id)
      context.log.debug("Dispatching task {} to {}", job, replyTo)
      jobType match {
        case JobType.Split =>
          val splitCandidates = id & taskState.splitCandidates
          replyTo ! CheckSplitCandidates(id, splitCandidates)
        case JobType.Swap =>
          val swapCandidates = taskState.swapCandidates
          replyTo ! CheckSwapCandidates(id, swapCandidates)
      }
      timingSpans.end("Helper dispatch work")
      Behaviors.same

    case CancelWork(id, jobType) =>
      master ! EnqueueCancelledJob(id, jobType)
      Behaviors.same

    case SplitCandidatesChecked(id, removedSplitCandidates) =>
      val job = id -> JobType.Split
      val stateUpdate = CandidateState.SplitChecked(removedSplitCandidates)
      updateState(job, stateUpdate)

    case SwapCandidatesChecked(id, removedSwapCandidates) =>
      val job = id -> JobType.Swap
      val stateUpdate = CandidateState.SwapChecked(removedSwapCandidates)
      updateState(job, stateUpdate)

    case GenerateCandidates(id, jobType, successorStates) =>
      context.log.debug("Generating successor candidates for job {}", id -> jobType)
      timingSpans.begin("Candidate generation")
      val (newJobs, newStateUpdates) = generateCandidates(jobType, successorStates)
      master ! NewCandidatesGenerated(id, jobType, newJobs, newStateUpdates)
      timingSpans.end("Candidate generation")
      Behaviors.same

    case InitWithAttributes(_) => // ignore
      Behaviors.same
  }

  private def updateState(
      job: (CandidateSet, JobType.JobType), stateUpdate: CandidateState.Delta
  ): Behavior[Command] = {
    context.log.debug("Received results for {}", job)
    timingSpans.begin("Result state update")
    val (id, jobType) = job
    var stateUpdates: PendingJobMap[CandidateSet, CandidateState.Delta] = PendingJobMap.empty
    var removePruned: Set[CandidateSet] = Set.empty

    // update state based on received results
    val newTaskState = state.get(id) match {
      case None => None
      case Some(s) =>
        stateUpdates += id -> stateUpdate
        val updated = s.updated(stateUpdate)
        if (updated.shouldBePruned) {
          stateUpdates += id -> Prune()
          Some(updated.updated(Prune()))
        } else {
          Some(updated)
        }
    }

    // perform pruning or update successors
    val successors = id.successors(attributes)
    val nodeIsPruned = newTaskState.forall(_.isPruned)

    if (nodeIsPruned) {
      context.log.debug("Pruning node {} and all successors", id)
      // node pruning! --> invalidate all successing nodes
      successors.foreach(s =>
        stateUpdates += s -> Prune()
      )
      // remove all jobs that involve one of the pruned successors
      removePruned ++= successors
    } else {
      context.log.debug("Updating successors for job {}: {}", job, successors)
      successors.foreach(successor =>
        stateUpdates += successor -> IncPrecondition(jobType)
      )
    }

    val distinctStateUpdates = stateUpdates.toMap.map {
      case (key, value) => key -> value.toSet
    }
    master ! UpdateState(job, distinctStateUpdates, removePruned, nodeIsPruned)
    timingSpans.end("Result state update")
    Behaviors.same
  }

  private def generateCandidates(
      jobType: JobType.JobType,
      successorStates: Set[CandidateState],
  ): (Set[(CandidateSet, JobType.JobType)], Map[CandidateSet, Set[CandidateState.Delta]]) = {
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
      .map { case (key, value) => key -> value.map { case (_, delta) => delta } }

    val newJobs: Set[(CandidateSet, JobType.JobType)] = newSplitJobs ++ newSwapJobs
    newJobs -> stateUpdates
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
