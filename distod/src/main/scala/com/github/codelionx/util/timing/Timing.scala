package com.github.codelionx.util.timing

import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors

import scala.util.control.NonFatal


// messages
sealed trait Command

final case class AddTiming(label: String, t0: Long, t1: Long) extends Command

final case class AddDuration(label: String, nanoDuration: Long) extends Command

case object Print extends Command


// extension constructor
object Timing extends ExtensionId[Timing] {

  override def createExtension(system: ActorSystem[_]): Timing = new Timing(system)
}


// extension implementation
class Timing(system: ActorSystem[_]) extends Extension {

  // cluster singleton ???
  val ref: ActorRef[Command] = system.systemActorOf(
    aggregatingTimingActor,
    "timeKeeper"
  )

  def unsafeTime[R](label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()

    ref ! AddTiming(label, t0, t1)
    result
  }

  def time[R](label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    try {
      val result = block
      result
    } finally {
      ref ! AddTiming(label, t0, System.nanoTime())
    }
  }

  def spans: TimingSpans = new TimingSpans(Map.empty, ref)

  def startSpan(label: String): TimingSpans = new TimingSpans(Map(label -> System.nanoTime()), ref)

  private def aggregatingTimingActor: Behavior[Command] = Behaviors.setup { _ =>
    val timings = new AggregatingTiming

    Behaviors
      .receiveMessage[Command] {
        case AddTiming(label, t0, t1) =>
          timings.addTiming(label, t0, t1)
          Behaviors.same
        case AddDuration(label, nanoDuration) =>
          timings.addTiming(label, nanoDuration)
          Behaviors.same
        case Print =>
          timings.print()
          Behaviors.stopped
      }
      .receiveSignal {
        case (_, PostStop) =>
          timings.print()
          Behaviors.same
      }
  }
}
