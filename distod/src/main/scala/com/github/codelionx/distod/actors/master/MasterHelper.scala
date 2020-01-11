package com.github.codelionx.distod.actors.master

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, PoolRouter, Routers}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import com.github.codelionx.distod.actors.master.Master.NewCandidates
import com.github.codelionx.distod.actors.master.MasterHelper.Command
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.largeMap.{CandidateTrie, HashMapState}


object MasterHelper {

  trait Command
  final case class GenerateSplitCandidates(
                                            candidateId: CandidateSet,
                                            state: HashMapState[CandidateState]
                                          ) extends Command
  final case class GenerateSwapCandidates(
                                           candidateId: CandidateSet,
                                           state: HashMapState[CandidateState]
                                         ) extends Command

  val poolName: String = "master-helper-pool"

  def name(id: Int): String = s"master-helper-$id"

  def apply(master: ActorRef[Master.Command]): Behavior[Command] = Behaviors.setup(context =>
    new MasterHelper(context, master).start()
  )

  def pool(n: Int, master: ActorRef[Master.Command]): PoolRouter[Command] = Routers.pool(n)(
    Behaviors.supervise(apply(master)).onFailure(SupervisorStrategy.restart)
  )
}


class MasterHelper(context: ActorContext[Command], master: ActorRef[Master.Command]) extends CandidateGeneration {

  def start(): Behavior[Command] = Behaviors.receiveMessage {
    case MasterHelper.GenerateSplitCandidates(candidateId, state) =>
      val candidates = generateSplitCandidates(candidateId, state.view)
      master ! NewCandidates(
        candidateId,
        JobType.Split,
        CandidateState.NewSplitCandidates(candidates)
      )
      Behaviors.same
    case MasterHelper.GenerateSwapCandidates(candidateId, state) =>
      val candidates = generateSwapCandidates(candidateId, state.view)
      master ! NewCandidates(
        candidateId,
        JobType.Swap,
        CandidateState.NewSwapCandidates(candidates)
      )
      Behaviors.same
  }
}
