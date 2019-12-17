package com.github.codelionx.util.trie

import scala.annotation.tailrec
import scala.collection.{mutable, Factory, StrictOptimizedMapOps}
import scala.collection.immutable.SortedSet
import scala.language.implicitConversions


class PrefixHashMap[A, V]
  extends mutable.Map[SortedSet[A], V]
    with mutable.MapOps[SortedSet[A], V, mutable.Map, PrefixHashMap[A, V]]
    with StrictOptimizedMapOps[SortedSet[A], V, PrefixHashMap, PrefixHashMap[A, V]] {

  private val suffixes: mutable.Map[A, PrefixHashMap[A, V]] = mutable.Map.empty
  private var value: Option[V] = None

  override def get(key: SortedSet[A]): Option[V] =
    if (key.isEmpty) value
    else
    // key.head is safe here
      suffixes.get(key.head).flatMap(_.get(key.tail))

  @tailrec
  final def withPrefix(prefix: SortedSet[A]): PrefixHashMap[A, V] =
    if (prefix.isEmpty) this
    else {
      // prefix.head is safe here
      val head = prefix.head
      suffixes.get(head) match {
        case None =>
          suffixes.update(head, empty)
        case _ =>
      }
      suffixes(head).withPrefix(prefix.tail)
    }

  override def addOne(elem: (SortedSet[A], V)): PrefixHashMap.this.type = {
    withPrefix(elem._1).value = Some(elem._2)
    this
  }

  override def subtractOne(key: SortedSet[A]): PrefixHashMap.this.type = {
    if (key.isEmpty) {
      value = None
    } else {
      // key.head is safe here
      suffixes.get(key.head).flatMap(_.remove(key.tail))
    }
    this
  }

  override def iterator: Iterator[(SortedSet[A], V)] = {
//      implicit @implicitNotFound("No implicit value for Ordering[${A}] found to build iterator") ev: Ordering[A]
    implicit val orderingA: Ordering[A] = implicitly[Ordering[A]]
    val thisNode = for (
      v <- value.iterator
    ) yield (SortedSet.empty[A], v)
    val suffixesIter = for {
      (suffix, otherMap) <- suffixes.iterator
      (suffixPart, v) <- otherMap
    } yield (suffixPart.incl(suffix), v)
    thisNode ++ suffixesIter
  }

  // Overloading of transformation methods that should return a PrefixMap
  def map[W](f: ((SortedSet[A], V)) => (SortedSet[A], W)): PrefixHashMap[A, W] =
    strictOptimizedMap(PrefixHashMap.newBuilder, f)

  def flatMap[W](f: ((SortedSet[A], V)) => IterableOnce[(SortedSet[A], W)]): PrefixHashMap[A, W] =
    strictOptimizedFlatMap(PrefixHashMap.newBuilder, f)

  // Override `concat` and `empty` methods to refine their return type
//  override def concat[W >: V](suffix: IterableOnce[(SortedSet[A], W)]): PrefixHashMap[A, W] =
//    strictOptimizedConcat(suffix, PrefixHashMap.newBuilder)
//override def concat[W >: V](suffix: IterableOnce[(SortedSet[A], W)]): PrefixHashMap[A, W] =
//  strictOptimizedConcat(suffix, PrefixHashMap.newBuilder)

  override def empty: PrefixHashMap[A, V] = new PrefixHashMap

  // Members declared in scala.collection.mutable.Clearable
  override def clear(): Unit = suffixes.clear()

  // Members declared in scala.collection.IterableOps
  override protected def fromSpecific(coll: IterableOnce[(SortedSet[A], V)]): PrefixHashMap[A, V] =
    PrefixHashMap.from(coll)

  override protected def newSpecificBuilder: mutable.Builder[(SortedSet[A], V), PrefixHashMap[A, V]] =
    PrefixHashMap.newBuilder

  override def className = "PrefixHashMap"
}

object PrefixHashMap {

  def empty[A, V]: PrefixHashMap[A, V] = new PrefixHashMap[A, V]

  def from[A, V](it: IterableOnce[(SortedSet[A], V)]): PrefixHashMap[A, V] = it match {
    case alreadyPrefixMap: PrefixHashMap[A, V] => alreadyPrefixMap
    case _ => (newBuilder ++= it).result()
  }

  def newBuilder[A, V]: mutable.Builder[(SortedSet[A], V), PrefixHashMap[A, V]] =
    new mutable.GrowableBuilder[(SortedSet[A], V), PrefixHashMap[A, V]](empty)

  implicit def toFactory[A, V](self: this.type): Factory[(SortedSet[A], V), PrefixHashMap[A, V]] =
    new Factory[(SortedSet[A], V), PrefixHashMap[A, V]] {
      override def fromSpecific(it: IterableOnce[(SortedSet[A], V)]): PrefixHashMap[A, V] = self.from(it)

      override def newBuilder: mutable.Builder[(SortedSet[A], V), PrefixHashMap[A, V]] = self.newBuilder
    }
}
