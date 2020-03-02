package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{Behavior, BehaviorInterceptor, TypedActorContext}
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.master.JobType
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates, Command, Stop}
import com.github.codelionx.distod.types.CandidateSet


trait WorkerStopInterceptor extends BehaviorInterceptor[Command, Command] {

  protected var stopped: Boolean = false
  protected var openDispatches: Int = 0

  protected def cancel(candidateId: CandidateSet, jobType: JobType.JobType): Unit

  protected def cancelAll(): Unit

  private def stopOrElse(default: Behavior[Command]): Behavior[Command] = {
    if (openDispatches <= 0) {
      cancelAll()
      Behaviors.stopped
    } else {
      default
    }
  }

  override def aroundReceive(
      ctx: TypedActorContext[Command],
      msg: Command,
      target: BehaviorInterceptor.ReceiveTarget[Command]
  ): Behavior[Command] = msg match {
    case Stop =>
      stopped = true
      ctx.asScala.log.info("Cancelling current jobs before stopping, " +
        "this could lead to dead letters for message sent by the partition manager!"
      )
      stopOrElse(Behaviors.same)

    case CheckSplitCandidates(candidateId, _) if stopped =>
      openDispatches -= 1
      cancel(candidateId, JobType.Split)
      stopOrElse(Behaviors.same)

    case CheckSwapCandidates(candidateId, _) if stopped =>
      openDispatches -= 1
      cancel(candidateId, JobType.Swap)
      stopOrElse(Behaviors.same)

    case m if stopped =>
      stopOrElse(target(ctx, m))

    case m: CheckSplitCandidates =>
      openDispatches -= 1
      target(ctx, m)

    case m: CheckSwapCandidates =>
      openDispatches -= 1
      target(ctx, m)

    case m =>
      target(ctx, m)
  }

  def dispatchWorkFromMaster(): Unit
}