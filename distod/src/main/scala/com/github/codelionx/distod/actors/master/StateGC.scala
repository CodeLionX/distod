package com.github.codelionx.distod.actors.master

import java.util.concurrent.TimeUnit

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.largeMap.mutable.FastutilState

import scala.concurrent.duration._
import scala.language.postfixOps


object StateGC {

  sealed trait Command
  private case object Tick extends Command

  def name = "state-gc"

  // run with DispatcherSelector.fromConfig(s"$namespace.cpu-bound-tasks-dispatcher")
  def apply(
      state: FastutilState[CandidateState],
      attributes: Set[Int],
      master: ActorRef[Master.Command],
  ): Behavior[Command] = Behaviors.setup(context =>
    Behaviors.withTimers(timers =>
      new StateGC(context, timers, state, attributes, master).start()
    )
  )

  private val interval: FiniteDuration = 5 seconds
  private val timeLimit: FiniteDuration = 200 millis

}


class StateGC(
    context: ActorContext[StateGC.Command],
    timers: TimerScheduler[StateGC.Command],
    state: FastutilState[CandidateState],
    attributes: Set[Int],
    master: ActorRef[Master.Command],
) {

  import StateGC._


  private val checkRate = 500
  private var clearedUntil = 0
  private var iter: Iterator[(CandidateSet, CandidateState)] = state.iterator

  timers.startTimerWithFixedDelay("tick-timer", Tick, interval)

  def start(): Behavior[Command] = Behaviors.receiveMessage {
    case Tick =>
      repruneLevelsIteratively()
      sweepLevels()
      Behaviors.same
  }

  private def elapsedTime(startNanos: Long): FiniteDuration = {
    val elapsedNanos = System.nanoTime() - startNanos
    FiniteDuration(elapsedNanos, TimeUnit.NANOSECONDS)
  }

  private def repruneLevelsIteratively(): Unit = {
    val start = System.nanoTime()
    var count = 0
    var danglingPrune = Set.empty[CandidateSet]
    while (iter.hasNext && (count % checkRate != 0 || timeLimit > elapsedTime(start))) {
      val (id, s) = iter.next()
      if (!s.isFullyChecked && id.predecessors.exists(state.get(_).exists(_.isPruned))) {
        danglingPrune += id
      }
      count += 1
    }
    state.addAll(danglingPrune.map(id => id -> CandidateState.pruned(id)))
    if (!iter.hasNext) {
      iter = state.iterator
    }
    val end = System.nanoTime()
    context.log.trace(
      "Repruned {} nodes within {}",
      danglingPrune.size,
      FiniteDuration((end - start) / 1000000, TimeUnit.MILLISECONDS)
    )
  }

  private def repruneLevels(): Unit = {
    for (i <- clearedUntil until state.sizeLevels) {
      val danglingPrune = state.filterInLevel(i, { case (id, s) =>
        !s.isFullyChecked && id.predecessors.exists(state.get(_).exists(_.isPruned))
      })
      for ((id, _) <- danglingPrune) {
        state.update(id, CandidateState.pruned(id))
      }
    }
  }

  private def sweepLevels(): Unit = {
    val clearedUntilBefore = clearedUntil
    val completedLevels = for {
      i <- clearedUntil until state.sizeLevels
      if state.forallInLevel(i, (_, s) => s.isFullyChecked)
    } yield i
    for (i <- clearedUntil until state.sizeLevels if completedLevels.contains(i + 1)) {
      state.clearLevel(i)
      clearedUntil = i + 1
    }
    if (clearedUntil > clearedUntilBefore)
      context.log.info("Cleared levels until: {}", clearedUntil - 1)
  }
}
