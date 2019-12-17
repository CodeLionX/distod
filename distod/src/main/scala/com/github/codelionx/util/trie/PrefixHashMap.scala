package com.github.codelionx.util.trie

import scala.annotation.tailrec
import scala.collection.{mutable, Factory, StrictOptimizedIterableOps}
import scala.collection.immutable.SortedSet
import scala.language.implicitConversions


class PrefixHashMap[A, K <: SortedSet[A], V]
  extends mutable.Map[K, V]
    with mutable.MapOps[K, V, mutable.Map, PrefixHashMap[A, K, V]]
    with StrictOptimizedIterableOps[(K, V), mutable.Iterable, PrefixHashMap[A, K, V]] {

  private val suffixes: mutable.Map[A, PrefixHashMap[A, K, V]] = mutable.Map.empty
  private var value: Option[V] = None

  override def get(key: K): Option[V] =
    if (key.isEmpty) value
    else
    // key.head is safe here
      suffixes.get(key.head).flatMap(_.get(key.tail.asInstanceOf[K]))

  @tailrec
  final def withPrefix(prefix: K): PrefixHashMap[A, K, V] =
    if (prefix.isEmpty) this
    else {
      // prefix.head is safe here
      val head = prefix.head
      suffixes.get(head) match {
        case None =>
          suffixes.update(head, empty)
        case _ =>
      }
      suffixes(head).withPrefix(prefix.tail.asInstanceOf[K])
    }

  override def addOne(elem: (K, V)): PrefixHashMap.this.type = {
    withPrefix(elem._1).value = Some(elem._2)
    this
  }

  override def subtractOne(key: K): PrefixHashMap.this.type = {
    if (key.isEmpty) {
      value = None
    } else {
      // key.head is safe here
      suffixes.get(key.head).flatMap(_.remove(key.tail.asInstanceOf[K]))
    }
    this
  }

  override def iterator: Iterator[(K, V)] = {
    val thisNode = for (
      v <- value.iterator
    ) yield (Iterator.empty[K], v)
    val suffixesIter = for {
      (suffix, otherMap) <- suffixes.iterator
      (suffixPart, v) <- otherMap
    } yield (suffixPart incl suffix, v)
    (thisNode ++ suffixesIter).asInstanceOf[Iterator[(K, V)]]
  }

  // Overloading of transformation methods that should return a PrefixMap
  def map[W](f: ((K, V)) => (K, W)): PrefixHashMap[A, K, W] =
    strictOptimizedMap(PrefixHashMap.newBuilder, f)

  def flatMap[W](f: ((K, V)) => IterableOnce[(K, W)]): PrefixHashMap[A, K, W] =
    strictOptimizedFlatMap(PrefixHashMap.newBuilder, f)

  // Override `concat` and `empty` methods to refine their return type
//  override def concat[W >: V](suffix: IterableOnce[(SortedSet[A], W)]): PrefixHashMap[A, W] =
//    strictOptimizedConcat(suffix, PrefixHashMap.newBuilder)
//override def concat[W >: V](suffix: IterableOnce[(SortedSet[A], W)]): PrefixHashMap[A, W] =
//  strictOptimizedConcat(suffix, PrefixHashMap.newBuilder)

  override def empty: PrefixHashMap[A, K, V] = new PrefixHashMap

  // Members declared in scala.collection.mutable.Clearable
  override def clear(): Unit = suffixes.clear()

  // Members declared in scala.collection.IterableOps
  override protected def fromSpecific(coll: IterableOnce[(K, V)]): PrefixHashMap[A, K, V] =
    PrefixHashMap.from[A, K, V](coll)

  override protected def newSpecificBuilder: mutable.Builder[(K, V), PrefixHashMap[A, K, V]] =
    PrefixHashMap.newBuilder

  override def className = "PrefixHashMap"
}

object PrefixHashMap {

  def empty[A, K <: SortedSet[A], V]: PrefixHashMap[A, K, V] = new PrefixHashMap[A, K, V]

  def from[A, K <: SortedSet[A], V](it: IterableOnce[(K, V)]): PrefixHashMap[A, K, V] = it match {
    case alreadyPrefixMap: PrefixHashMap[A, K, V] @unchecked => alreadyPrefixMap
    case _ => (newBuilder[A, K, V] ++= it).result()
  }

  def newBuilder[A, K <: SortedSet[A], V]: mutable.Builder[(K, V), PrefixHashMap[A, K, V]] =
    new mutable.GrowableBuilder[(K, V), PrefixHashMap[A, K, V]](empty)

  implicit def toFactory[A , K <: SortedSet[A], V](self: this.type): Factory[(K, V), PrefixHashMap[A, K, V]] =
    new Factory[(K, V), PrefixHashMap[A, K, V]] {
      override def fromSpecific(it: IterableOnce[(K, V)]): PrefixHashMap[A, K, V] = self.from(it)

      override def newBuilder: mutable.Builder[(K, V), PrefixHashMap[A, K, V]] = self.newBuilder
    }
}
