package com.github.codelionx.util.timing

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed._


// extension implementation
private[timing] class TimingImpl(system: ActorSystem[_]) extends Timing {

  // cluster singleton ???
  private val ref: ActorRef[Command] = system.systemActorOf(
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

  def spans: TimingSpans = TimingSpans(Map.empty, ref)

  def startSpan(label: String): TimingSpans = TimingSpans(Map(label -> System.nanoTime()), ref)

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
