package com.github.codelionx.util.largeMap.mutable

import org.agrona.collections.{Hashing, Object2ObjectHashMap}

import scala.collection.{IterableOnce, mutable}
import scala.jdk.CollectionConverters._


/**
 * A map that stores multiple values for a key in a [[scala.collection.Seq]]. It allows merging them together without
 * losing key bindings.
 */
object PendingJobMap {

  def empty[K, V]: PendingJobMap[K, V] = new PendingJobMap[K, V](new Object2ObjectHashMap())

  def from[K, V](it: IterableOnce[(K, Seq[V])]): PendingJobMap[K, V] = {
    val map = new Object2ObjectHashMap[K, mutable.ListBuffer[V]](it.knownSize, Hashing.DEFAULT_LOAD_FACTOR)
    for ((key, value) <- it) {
      map.put(key, value.to(mutable.ListBuffer))
    }
    new PendingJobMap[K, V](map)
  }

  def apply[K, V](elems: (K, Seq[V])*): PendingJobMap[K, V] = from(elems)
}


/**
 * A map that stores multiple values for a key in a [[scala.collection.Seq]]. It allows merging them together without
 * losing key bindings.
 *
 * @tparam K the type of the keys
 * @tparam V the type of the values that are stored in the `Seq` for each key
 */
class PendingJobMap[K, V] private (private val jobMap: Object2ObjectHashMap[K, mutable.ListBuffer[V]])
  extends IterableOnce[(K, Seq[V])] {

  /**
   * Retrieves the values which are associated with the given key. It throws a `NoSuchElementException` if the key
   * is not found.
   *
   * @param  key the key value
   * @return the values associated with the given key in a [[scala.collection.Seq]]
   */
  @throws[NoSuchElementException]
  @inline def apply(key: K): Seq[V] = jobMap.get(key) match {
    case null => throw new NoSuchElementException(s"$key not found")
    case value => value.toSeq
  }

  /**
   * Optionally returns the values associated with a key.
   *
   * @param  key the key value
   * @return an option value containing the value associated with `key` in this pending job map, or `None` if none
   *         exists.
   */
  @inline def get(key: K): Option[Seq[V]] = jobMap.get(key) match {
    case null => None
    case value => Some(value.toSeq)
  }

  /**
   * The size of this pending job map.
   *
   * @return the number of key
   */
  def size: Int = jobMap.size

  /**
   * Removes a key and all its values from this map.
   *
   * @param key the key to be removed
   * @return `this`
   */
  def removeKey(key: K): this.type =
    if (jobMap.isEmpty)
      this
    else {
      jobMap.remove(key)
      this
    }

  /**
   * Alias for `removed()`
   */
  @inline def -(kv: (K, V)): this.type = removed(kv._1, kv._2)

  /**
   * Removes a value from this map.
   *
   * @param key   the key where the value should be removed from
   * @param value the value to be removed
   * @return `this`
   */
  def removed(key: K, value: V): this.type = {
    jobMap.get(key) match {
      case null => this
      case seq if seq.isEmpty => this
      case seq =>
        seq.filterInPlace(_ != value)
        this
    }
  }

  /**
   * Alias for `updated()`
   */
  @inline def +(kv: (K, V)): this.type = updated(kv._1, kv._2)

  /**
   * Creates a new map obtained by updating this map with a given key/value pair.
   *
   * If a binding for `key` already exists the `value` will be added to the binding. If it not exists, a new key/value
   * mapping is added.
   *
   * @param    key   the key
   * @param    value the value
   * @return `this`
   */
  def updated(key: K, value: V): this.type = {
    if (jobMap.containsKey(key)) {
      jobMap.get(key).addOne(value)
    } else {
      jobMap.put(key, mutable.ListBuffer(value))
    }
    this
  }

  /**
   * Alias to `merge()` that also works for other collections ([[scala.collection.IterableOnce]]).
   */
  @inline def ++(xs: IterableOnce[(K, IterableOnce[V])]): this.type = {
    for ((key, value) <- xs) {
      if (jobMap.containsKey(key)) {
        jobMap.get(key).addAll(value)
      } else {
        jobMap.put(key, mutable.ListBuffer.from(value))
      }
    }
    this
  }

  /**
   * Alias to `merge()`
   */
  @inline def ++(other: PendingJobMap[K, V]): this.type = merge(other)

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
   * @return `this` including all bindings of both maps merged together.
   */
  def merge(other: PendingJobMap[K, V]): this.type = {
    if (!other.jobMap.isEmpty) {
      for (key <- other.jobMap.keySet().asScala) {
        if (jobMap.containsKey(key)) {
          jobMap.get(key).addAll(other.jobMap.get(key))
        } else {
          jobMap.put(key, other.jobMap.get(key))
        }
      }
    }
    this
  }

  /**
   * Collects all keys of this map in an iterable collection.
   *
   * @return the keys of this map as an iterable.
   */
  @inline def keys: Iterable[K] = jobMap.keySet().asScala

  /**
   * Collects all values of this map in an iterable collection.
   *
   * @return the values of this map as an nested iterable.
   */
  @inline def values: Iterable[Seq[V]] = jobMap.values().asScala.map(_.toSeq)

  /**
   * Tests whether this map contains a binding for a key.
   *
   * @param key the key
   * @return `true` if there is a binding for `key` in this map, `false` otherwise.
   */
  @inline def contains(key: K): Boolean = jobMap.containsKey(key)

  @inline def mkString(sep: String): String = jobMap.asScala.mkString(sep)

  @inline def toMap[K2, V2](implicit ev: (K, mutable.ListBuffer[V]) <:< (K2, mutable.ListBuffer[V2])): Map[K2, mutable.ListBuffer[V2]] =
    jobMap.asScala.toMap[K2, mutable.ListBuffer[V2]]

  override def iterator: Iterator[(K, Seq[V])] =
    jobMap.asScala.map{ case (key, value) => key -> value.toSeq }.iterator

  // Object overwrites
  override def toString: String = s"PendingJobMap(${mkString(", ")})"

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

  def copy(): PendingJobMap[K, V] = PendingJobMap.from(this)
}
