package com.github.codelionx.distod.types

import akka.util.TypedMultiMap

import scala.collection.IterableOnce


object PendingJobMap {

  def empty[K, V]: PendingJobMap[K, V] = new PendingJobMap[K, V](Map.empty)

  def from[K, V](it: IterableOnce[(K, Seq[V])]): PendingJobMap[K, V] = new PendingJobMap[K, V](Map.from(it))

  def apply[K, V](elems: (K, Seq[V])*): PendingJobMap[K, V] = from(elems)
}

class PendingJobMap[K, +V](private val jobMap: Map[K, Seq[V]]) {

  TypedMultiMap
  Map(1 -> 2) + (1 -> 2)

  def apply(key: K): Seq[V] = jobMap(key)

  def get(key: K): Option[Seq[V]] = jobMap.get(key)

  def keyRemoved(key: K): PendingJobMap[K, V] = new PendingJobMap(jobMap - key)

  def -[V1 >: V](kv: (K, V1)): PendingJobMap[K, V1] = removed(kv._1, kv._2)

  def removed[V1 >: V](key: K, value: V1): PendingJobMap[K, V1] = {
    jobMap.get(key) match {
      case None => this
      case Some(seq) if seq.isEmpty => this
      case Some(seq) =>
        val without = seq.filterNot(_ == value)
        new PendingJobMap(jobMap.updated(key, without))
    }
  }

  def +[V1 >: V](kv: (K, V1)): PendingJobMap[K, V1] = updated(kv._1, kv._2)

  def updated[V1 >: V](key: K, value: V1): PendingJobMap[K, V1] = {
    if (jobMap.contains(key)) {
      val newSeq = jobMap(key) :+ value
      new PendingJobMap(jobMap.updated(key, newSeq))
    } else {
      new PendingJobMap[K, V1](jobMap.updated(key, Seq(value)))
    }
  }

  def ++[V1 >: V](xs: IterableOnce[(K, Seq[V1])]): PendingJobMap[K, V1] =
    merge(new PendingJobMap[K, V1](Map.from(xs)))

  def ++[V1 >: V](other: PendingJobMap[K, V1]): PendingJobMap[K, V1] = merge(other)

  def merge[V1 >: V](other: PendingJobMap[K, V1]): PendingJobMap[K, V1] = {
    if (jobMap.isEmpty)
      other
    else if (other.jobMap.isEmpty)
      this
    else {
      val updatedMap = (jobMap.keys ++ other.jobMap.keys).map { key =>
        val ours = jobMap.get(key)
        val theirs = other.jobMap.get(key)
        (ours, theirs) match {
          case (None, None) => key -> Seq.empty[V1]
          case (Some(items), None) => key -> items
          case (None, Some(items)) => key -> items
          case (Some(ourItems), Some(theirItems)) => key -> ourItems.prependedAll(theirItems)
        }
      }
      new PendingJobMap(updatedMap.toMap)
    }
  }

  def contains(key: K): Boolean = jobMap.contains(key)

  def mkString(sep: String): String = jobMap.mkString(sep)

  override def toString: String = s"PendingJobMap(${jobMap.mkString(", ")})"
}
