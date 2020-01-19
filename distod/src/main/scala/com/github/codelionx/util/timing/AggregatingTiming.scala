package com.github.codelionx.util.timing

import scala.collection.mutable


/**
 * MUTABLE!!
 */
class AggregatingTiming private[timing]() {

  private val timings: mutable.Map[String, Long] = mutable.Map.empty

  def addTiming(label: String, t0: Long, t1: Long): Unit = addTiming(label, t1 - t0)

  def addTiming(label: String, nanoDuration: Long): Unit = {
    timings.updateWith(label) {
      case Some(value) => Some(value + nanoDuration)
      case None => Some(nanoDuration)
    }
  }

  // faster, but fails to record time on exceptional code
  def unsafeTime[R](label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()

    addTiming(label, t0, t1)
    result
  }

  // slower, but handles exceptions correctly
  def time[R](label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    try {
      val result = block
      result
    } finally {
      addTiming(label, t0, System.nanoTime())
    }
  }

  def print(): Unit = {
    timings.toSeq
      .sortBy(_._2)
      .reverse
      .foreach { case (label, nanoDuration) =>
        val duration = (nanoDuration / 1e6).toLong
        println(s"$MESSAGE_PREFIX $label: $duration ms")
      }
  }
}
