package com.github.codelionx.distod.actors.master

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, PoolRouter, Routers}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import com.github.codelionx.distod.actors.master.Master.NewCandidatesGenerated
import com.github.codelionx.distod.actors.worker.Worker
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates}
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.largeMap.mutable.FastutilState
import com.github.codelionx.util.timing.Timing

import scala.concurrent.duration._
import scala.language.postfixOps


object MasterHelper {

  sealed trait Command
  final case class DispatchWorkTo(id: CandidateSet, jobType: JobType.JobType, worker: ActorRef[Worker.Command])
    extends Command
  final case class GenerateCandidates(id: CandidateSet, jobType: JobType.JobType, successorStates: Set[CandidateState])
    extends Command

  val poolName = "master-helper-pool"

  def name(n: Int): String = s"master-helper-$n"

  def createPool(n: Int)(state: FastutilState[CandidateState], master: ActorRef[Master.Command]): PoolRouter[Command] = Routers.pool(n)(
    Behaviors.supervise(apply(state, master)).onFailure[Exception](
      SupervisorStrategy.restart
        .withLoggingEnabled(true)
        .withLimit(3, 10 seconds)
    )
  ).withRoundRobinRouting()

  private def apply(state: FastutilState[CandidateState], master: ActorRef[Master.Command]): Behavior[Command] = Behaviors.setup(context =>
    new MasterHelper(context, state, master).start()
  )
}

class MasterHelper(
    context: ActorContext[MasterHelper.Command],
    state: FastutilState[CandidateState],
    master: ActorRef[Master.Command]
) extends CandidateGeneration {

  import MasterHelper._

  private val timingSpans = Timing(context.system).spans

  def start(): Behavior[Command] = Behaviors.receiveMessage{
    case DispatchWorkTo(id, jobType, worker) =>
      timingSpans.start("Helper dispatch work")
      val taskState = state(id)
      context.log.debug("Dispatching task {} to {}", id -> jobType, worker)
      jobType match {
        case JobType.Split =>
          val splitCandidates = id & taskState.splitCandidates
          worker ! CheckSplitCandidates(id, splitCandidates)
        case JobType.Swap =>
          val swapCandidates = taskState.swapCandidates
          worker ! CheckSwapCandidates(id, swapCandidates)
      }
      timingSpans.end("Helper dispatch work")
      Behaviors.same

    case GenerateCandidates(id, jobType, successorStates) =>
      timingSpans.start("Candidate generation")
      context.log.debug("Generating successor candidates for job {}: {}", id -> jobType, successorStates.map(_.id))
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

      val newJobs: Set[(CandidateSet, JobType.JobType)] = newSplitJobs ++ newSwapJobs
      master ! NewCandidatesGenerated(id, jobType, newJobs, stateUpdates)
      timingSpans.end("Candidate generation")
      Behaviors.same
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
