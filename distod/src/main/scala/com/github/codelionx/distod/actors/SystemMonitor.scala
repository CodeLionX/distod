package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.codelionx.distod.actors.SystemMonitor._
import com.github.codelionx.distod.Settings
import com.github.codelionx.util.GenericLogLevelLogger._


object SystemMonitor {

  sealed trait Command
  final case class Register(ref: ActorRef[SystemEvent]) extends Command
  private case object Tick extends Command
  private case object StatisticsTick extends Command

  sealed trait SystemEvent
  final case object CriticalHeapUsage extends SystemEvent

  val name = "system-monitor"

  def apply(): Behavior[Command] = Behaviors.setup(context =>
    Behaviors.withTimers(timer =>
      new SystemMonitor(context, timer).start()
    )
  )
}


class SystemMonitor(context: ActorContext[Command], timer: TimerScheduler[Command]) {

  private final val megabyte: Int = 1024*1024

  private val settings = Settings(context.system).monitoringSettings
  private val runtime = Runtime.getRuntime

  if(context.log.isEnabled(settings.statisticsLogLevel)) {
    timer.startTimerWithFixedDelay("statistics-tick", StatisticsTick, settings.statisticsLogInterval)
  }
  timer.startTimerWithFixedDelay("tick", Tick, settings.interval)

  def start(): Behavior[Command] = behavior(Set.empty)

  var free: Long = 0
  var total: Long = 0
  var usageP: Double = .0
  var max: Long = 0

  def behavior(listeners: Set[ActorRef[SystemEvent]]): Behavior[Command] = Behaviors.receiveMessage {
    case Register(ref) =>
      behavior(listeners + ref)

    case Tick =>
      updateStatistics()
      if (usageP > settings.heapEvictionThreshold) {
        logStatistics("CriticalHeapUsage event triggered!")
        listeners.foreach(_ ! CriticalHeapUsage)
        waitForGC(listeners)
      } else {
        Behaviors.same
      }

    case StatisticsTick =>
      logStatistics()
      Behaviors.same
  }

  def waitForGC(listeners: Set[ActorRef[SystemEvent]], waitingTicks: Int = 3): Behavior[Command] = Behaviors.receiveMessage{
    case Register(ref) =>
      waitForGC(listeners + ref)

    case Tick =>
      updateStatistics()
      val newWaitingTicks = waitingTicks - 1
      if (usageP > settings.heapEvictionThreshold || newWaitingTicks > 0) {
        waitForGC(listeners, newWaitingTicks)
      } else {
        max = 0
        behavior(listeners)
      }

    case StatisticsTick =>
      logStatistics()
      Behaviors.same
  }

  private def updateStatistics(): Unit = {
    free = runtime.freeMemory()
    total = runtime.totalMemory()
    val usage = total - free
    usageP = usage.toDouble / total.toDouble
    max = scala.math.max(max, usage)
  }

  private def logStatistics(prefixMessage: String = ""): Unit = {
    context.log.log(
      settings.statisticsLogLevel,
      s"Heap usage: {} % [free={} mb, total={} mb, max={} mb] ${prefixMessage}",
      scala.math.ceil(usageP * 100),
      free / megabyte,
      total / megabyte,
      max / megabyte
    )
  }
}
