package com.github.codelionx.util.timing


private[timing] object NoOpTimingImpl extends Timing {

  override def unsafeTime[R](label: String)(block: => R): R = block

  override def time[R](label: String)(block: => R): R = block

  override def createSpans: TimingSpans = TimingSpans.NoOpTimingSpans

  override def startSpan(label: String): TimingSpans = TimingSpans.NoOpTimingSpans
}
