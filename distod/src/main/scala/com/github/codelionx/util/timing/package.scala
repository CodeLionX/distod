package com.github.codelionx.util


package object timing {

  final val MESSAGE_PREFIX = "TIME"

  private def duration(start: Long, end: Long): Long =
    ((end - start) / 1e6).toLong

  def printTime(label: String, t0: Long, t1: Long = System.nanoTime()): Unit = {
    println(s"$MESSAGE_PREFIX $label: ${duration(t0, t1)} ms")
  }

  def unsafeTime[R](label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    printTime(label, t0)
    result
  }

  def time[R](label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    try {
      val result = block
      result
    } finally {
      printTime(label, t0)
    }
  }
}
