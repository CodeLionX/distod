package com.github.codelionx.distod.types

import scala.collection.{Iterable, IterableOnce}


/**
 * A map that stores multiple values for a key in a [[scala.collection.Seq]]. It allows merging them together without
 * losing key bindings.
 */
object PendingJobMap {

  def empty[K, V]: PendingJobMap[K, V] = new PendingJobMap[K, V](Map.empty)

  def from[K, V](it: IterableOnce[(K, Seq[V])]): PendingJobMap[K, V] = new PendingJobMap[K, V](Map.from(it))

  def apply[K, V](elems: (K, Seq[V])*): PendingJobMap[K, V] = from(elems)
}


/**
 * A map that stores multiple values for a key in a [[scala.collection.Seq]]. It allows merging them together without
 * losing key bindings.
 *
 * @tparam K the type of the keys
 * @tparam V the type of the values that are stored in the `Seq` for each key
 */
class PendingJobMap[K, +V](private val jobMap: Map[K, Seq[V]]) {

  /**
   * Retrieves the values which are associated with the given key. It throws a `NoSuchElementException` if the key
   * is not found.
   *
   * @param  key the key value
   * @return the values associated with the given key in a [[scala.collection.Seq]]
   */
  @throws[NoSuchElementException]
  @inline def apply(key: K): Seq[V] = jobMap(key)

  /**
   * Optionally returns the values associated with a key.
   *
   * @param  key the key value
   * @return an option value containing the value associated with `key` in this pending job map, or `None` if none
   *         exists.
   */
  @inline def get(key: K): Option[Seq[V]] = jobMap.get(key)

  /**
   * The size of this pending job map.
   *
   * @return the number of key
   */
  def size: Int = jobMap.size

  /**
   * Removes a key and all its values from this map, returning a new map.
   *
   * @param key the key to be removed
   * @return a new map without a binding for `key`
   */
  def keyRemoved(key: K): PendingJobMap[K, V] =
    if (jobMap.isEmpty)
      this
    else
      new PendingJobMap(jobMap - key)

  /**
   * Alias for `removed()`
   */
  @inline def -[V1 >: V](kv: (K, V1)): PendingJobMap[K, V1] = removed(kv._1, kv._2)

  /**
   * Removes a value from this map, returning a new map.
   *
   * @param key   the key where the value should be removed from
   * @param value the value to be removed
   * @return a new map where `value` was removed from the binding for `key`
   */
  def removed[V1 >: V](key: K, value: V1): PendingJobMap[K, V1] = {
    jobMap.get(key) match {
      case None => this
      case Some(seq) if seq.isEmpty => this
      case Some(seq) =>
        val without = seq.filterNot(_ == value)
        new PendingJobMap(jobMap.updated(key, without))
    }
  }

  /**
   * Alias for `updated()`
   */
  @inline def +[V1 >: V](kv: (K, V1)): PendingJobMap[K, V1] = updated(kv._1, kv._2)

  /**
   * Creates a new map obtained by updating this map with a given key/value pair.
   *
   * If a binding for `key` already exists the `value` will be added to the binding. If it not exists, a new key/value
   * mapping is added.
   *
   * @param    key   the key
   * @param    value the value
   * @tparam   V1 the type of the added value
   * @return A new map with the new key/value mapping added to this map.
   */
  def updated[V1 >: V](key: K, value: V1): PendingJobMap[K, V1] = {
    if (jobMap.contains(key)) {
      val newSeq = jobMap(key) :+ value
      new PendingJobMap(jobMap.updated(key, newSeq))
    } else {
      new PendingJobMap[K, V1](jobMap.updated(key, Seq(value)))
    }
  }

  /**
   * Alias to `merge()` that also works for other collections ([[scala.collection.IterableOnce]]).
   */
  @inline def ++[V1 >: V](xs: IterableOnce[(K, Seq[V1])]): PendingJobMap[K, V1] =
    merge(new PendingJobMap[K, V1](Map.from(xs)))

  /**
   * Alias to `merge()`
   */
  @inline def ++[V1 >: V](other: PendingJobMap[K, V1]): PendingJobMap[K, V1] = merge(other)

  /**
   * Merges two `PendingJobMap`s so that every key of both maps is included in the new map and the bindings for keys in
   * both maps are concatenated together (this concat other).
   *
   * @example {{{
   * val map1 = PendingJobMap(1 -> Seq(0,1))
   * val map2 = PendingJobMap(0 -> Seq(4), 1 -> Seq(2,3))
   * map1.merge(map2) == PendingJobMap(
   *   0 -> Seq(4),
   *   1 -> Seq(0,1,2,3)
   * )
   * }}}
   * @param other the other pending job map
   * @tparam V1 the value type of the other pending job map
   * @return A new map with all bindings of both maps merged together.
   */
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
          case (None, None) => key -> Seq.empty[V1] // should not occur
          case (Some(items), None) => key -> items
          case (None, Some(items)) => key -> items
          case (Some(ourItems), Some(theirItems)) => key -> ourItems.appendedAll(theirItems)
        }
      }
      new PendingJobMap(updatedMap.toMap)
    }

  }

  /**
   * Collects all keys of this map in an iterable collection.
   *
   * @return the keys of this map as an iterable.
   */
  @inline def keys: Iterable[K] = jobMap.keys

  /**
   * Collects all values of this map in an iterable collection.
   *
   * @return the values of this map as an nested iterable.
   */
  @inline def values: Iterable[Seq[V]] = jobMap.values

  /**
   * Tests whether this map contains a binding for a key.
   *
   * @param key the key
   * @return `true` if there is a binding for `key` in this map, `false` otherwise.
   */
  @inline def contains(key: K): Boolean = jobMap.contains(key)

  @inline def mkString(sep: String): String = jobMap.mkString(sep)

  @inline def toMap[K2, V2](implicit ev: (K, Seq[V]) <:< (K2, Seq[V2])): Map[K2, Seq[V2]] = jobMap.toMap[K2, Seq[V2]]

  // Object overwrites
  override def toString: String = s"PendingJobMap(${jobMap.mkString(", ")})"

  def canEqual(other: Any): Boolean = other.isInstanceOf[PendingJobMap[_, _]]

  override def equals(other: Any): Boolean = other match {
    case that: PendingJobMap[_, _] =>
      (that canEqual this) &&
        jobMap.equals(that.jobMap)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(jobMap)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
