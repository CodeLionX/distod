package com.github.codelionx.util.timing

import akka.actor.typed._


// messages
sealed trait Command

final case class AddTiming(label: String, t0: Long, t1: Long) extends Command

final case class AddDuration(label: String, nanoDuration: Long) extends Command

case object Print extends Command


// extension constructor
object Timing extends ExtensionId[Timing] {

  override def createExtension(system: ActorSystem[_]): Timing =
    if (GATHER_TIMINGS)
      new TimingImpl(system)
    else
      NoOpTimingImpl
}

trait Timing extends Extension {

  def unsafeTime[R](label: String)(block: => R): R

  def time[R](label: String)(block: => R): R

  def spans: TimingSpans

  def startSpan(label: String): TimingSpans
}
