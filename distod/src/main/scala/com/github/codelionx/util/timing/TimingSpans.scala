package com.github.codelionx.util.timing

import akka.actor.typed.ActorRef


class TimingSpans private[timing](private var begins: Map[String, Long], ref: ActorRef[Command]) {

  /**
   * Alias to `start`
   * @see [[com.github.codelionx.util.timing.TimingSpans#start]]
   */
  def begin(label: String): Unit = start(label)

  def start(label: String): Unit = {
    val start = System.nanoTime()
    begins += label -> start
  }

  def end(label: String): Unit = {
    val end = System.nanoTime()
    val start = begins.getOrElse(label, end)
    ref ! AddTiming(label, start, end)
  }
}
