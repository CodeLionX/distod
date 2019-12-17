package com.github.codelionx.util.trie

import com.github.codelionx.distod.types.CandidateSet

import scala.annotation.tailrec
import scala.collection.{mutable, Factory, StrictOptimizedIterableOps}
import scala.language.implicitConversions


class CandidatePrefixTrie[V]
  extends mutable.Map[CandidatePrefixTrie.K, V]
    with mutable.MapOps[CandidatePrefixTrie.K, V, mutable.Map, CandidatePrefixTrie[V]]
    with StrictOptimizedIterableOps[(CandidatePrefixTrie.K, V), mutable.Iterable, CandidatePrefixTrie[V]] {

  import CandidatePrefixTrie._


  private val suffixes: mutable.Map[A, CandidatePrefixTrie[V]] = mutable.Map.empty
  private var value: Option[V] = None

  override def get(key: K): Option[V] =
    if (key.isEmpty) value
    else
    // key.head is safe here
      suffixes.get(key.head).flatMap(_.get(key.tail))

  @tailrec
  final def withPrefix(prefix: K): CandidatePrefixTrie[V] =
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

  override def addOne(elem: (K, V)): CandidatePrefixTrie.this.type = {
    withPrefix(elem._1).value = Some(elem._2)
    this
  }

  override def subtractOne(key: K): CandidatePrefixTrie.this.type = {
    if (key.isEmpty) {
      value = None
    } else {
      // key.head is safe here
      suffixes.get(key.head).flatMap(_.remove(key.tail))
    }
    this
  }

  override def iterator: Iterator[(K, V)] = {
    val thisNode = for (
      v <- value.iterator
    ) yield (keyFactory.fromSpecific(Iterable.empty), v)
    val suffixesIter = for {
      (suffix, otherMap) <- suffixes.iterator
      (suffixPart, v) <- otherMap
    } yield (suffixPart incl suffix, v)
    thisNode ++ suffixesIter
  }

  // Overloading of transformation methods that should return a CandidatePrefixTrie
  def map[W](f: ((K, V)) => (K, W)): CandidatePrefixTrie[W] =
    strictOptimizedMap(CandidatePrefixTrie.newBuilder, f)

  def flatMap[W](f: ((K, V)) => IterableOnce[(K, W)]): CandidatePrefixTrie[W] =
    strictOptimizedFlatMap(CandidatePrefixTrie.newBuilder, f)

  // Override `concat` and `empty` methods to refine their return type
  override def concat[W >: V](suffix: IterableOnce[(K, W)]): CandidatePrefixTrie[W] =
    strictOptimizedConcat(suffix, CandidatePrefixTrie.newBuilder)

  override def empty: CandidatePrefixTrie[V] = new CandidatePrefixTrie

  // Members declared in scala.collection.mutable.Clearable
  override def clear(): Unit = {
    suffixes.clear()
    value = None
  }

  // Members declared in scala.collection.IterableOps
  override protected def fromSpecific(coll: IterableOnce[(K, V)]): CandidatePrefixTrie[V] =
    CandidatePrefixTrie.from[V](coll)

  override protected def newSpecificBuilder: mutable.Builder[(K, V), CandidatePrefixTrie[V]] =
    CandidatePrefixTrie.newBuilder

  override def className = "CandidatePrefixTrie"
}


object CandidatePrefixTrie {

  /**
   * Key-Element type
   */
  private type A = Int
  /**
   * Key type
   */
  private type K = CandidateSet

  /**
   * Factory for keys (using key elements)
   */
  private def keyFactory: Factory[A, K] = CandidateSet

  def empty[V]: CandidatePrefixTrie[V] = new CandidatePrefixTrie[V]

  def apply[V](elems: (K, V)*): CandidatePrefixTrie[V] = from(elems)

  def from[V](it: IterableOnce[(K, V)]): CandidatePrefixTrie[V] = it match {
    case alreadyPrefixMap: CandidatePrefixTrie[V] => alreadyPrefixMap
    case _ => (newBuilder[V] ++= it).result()
  }

  def newBuilder[V]: mutable.Builder[(K, V), CandidatePrefixTrie[V]] =
    new mutable.GrowableBuilder[(K, V), CandidatePrefixTrie[V]](empty)

  implicit def toFactory[V](self: this.type): Factory[(K, V), CandidatePrefixTrie[V]] =
    new Factory[(K, V), CandidatePrefixTrie[V]] {
      override def fromSpecific(it: IterableOnce[(K, V)]): CandidatePrefixTrie[V] = self.from(it)

      override def newBuilder: mutable.Builder[(K, V), CandidatePrefixTrie[V]] = self.newBuilder
    }
}
