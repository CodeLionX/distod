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
    timer.startTimerWithFixedDelay("tick", Tick, settings.interval)
  }
  timer.startTimerWithFixedDelay("statistics-tick", StatisticsTick, settings.statisticsLogInterval)

  def start(): Behavior[Command] = behavior(Set.empty)

  var free: Long = 0
  var total: Long = 0
  var usageP: Double = .0
  var max: Long = 0

  def behavior(listeners: Set[ActorRef[SystemEvent]]): Behavior[Command] = Behaviors.receiveMessage {
    case Register(ref) =>
      behavior(listeners + ref)

    case Tick =>
      free = runtime.freeMemory()
      total = runtime.maxMemory()
      val usage = total - free
      usageP = usage.toDouble / total.toDouble
      if (usageP > settings.heapEvictionThreshold) {
        listeners.foreach(_ ! CriticalHeapUsage)
      }
      max = scala.math.max(max, usage)
      Behaviors.same

    case StatisticsTick =>
      context.log.log(
        settings.statisticsLogLevel,
        "Heap usage: {} % [free={} mb, total={} mb, max={} mb]",
        scala.math.ceil(usageP * 100),
        free / megabyte,
        total / megabyte,
        max / megabyte
      )
      Behaviors.same
  }
}
