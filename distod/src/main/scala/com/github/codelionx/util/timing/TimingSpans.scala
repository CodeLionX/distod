package com.github.codelionx.util.timing

import akka.actor.typed.ActorRef


trait TimingSpans {

  /**
   * Alias to `start`
   *
   * @see [[com.github.codelionx.util.timing.TimingSpans#start]]
   */
  def begin(label: String): Unit = start(label)

  /**
   * Alias to `stop`
   *
   * @see [[com.github.codelionx.util.timing.TimingSpans#stop]]
   */
  def end(label: String): Unit = stop(label)

  def start(label: String): Unit

  def stop(label: String): Unit
}

object TimingSpans {

  private[timing] def apply(begins: Map[String, Long], ref: ActorRef[Command]): TimingSpans =
    new TimingSpansImpl(begins, ref)

  class TimingSpansImpl private[TimingSpans](private var begins: Map[String, Long], ref: ActorRef[Command])
    extends TimingSpans {

    def start(label: String): Unit = {
      val start = System.nanoTime()
      begins += label -> start
    }

    def stop(label: String): Unit = {
      val end = System.nanoTime()
      val start = begins.getOrElse(label, end)
      ref ! AddTiming(label, start, end)
    }
  }

  private[timing] object NoOpTimingSpans extends TimingSpans {
    override def start(label: String): Unit = ()

    override def stop(label: String): Unit = ()
  }

}