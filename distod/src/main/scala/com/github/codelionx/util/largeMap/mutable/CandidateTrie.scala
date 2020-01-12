package com.github.codelionx.util.largeMap.mutable

import com.github.codelionx.distod.types.CandidateSet

import scala.annotation.tailrec
import scala.collection.{mutable, Factory, StrictOptimizedIterableOps}
import scala.language.implicitConversions


class CandidateTrie[V]
  extends mutable.Map[CandidateTrie.K, V]
    with mutable.MapOps[CandidateTrie.K, V, mutable.Map, CandidateTrie[V]]
    with StrictOptimizedIterableOps[(CandidateTrie.K, V), mutable.Iterable, CandidateTrie[V]] {

  import CandidateTrie._


  private val suffixes: mutable.Map[A, CandidateTrie[V]] = mutable.Map.empty
  private var value: Option[V] = None

  override def get(key: K): Option[V] = key.headOption match {
    case None =>
      value
    case Some(head) =>
      suffixes.get(head).flatMap(_.get(key.tail))
  }

  @tailrec
  final def withPrefix(prefix: K): CandidateTrie[V] = prefix.headOption match {
    case None => this
    case Some(head) =>
      suffixes.get(head) match {
        case None =>
          suffixes.update(head, empty)
        case _ =>
      }
      suffixes(head).withPrefix(prefix.tail)
  }

  override def addOne(elem: (K, V)): CandidateTrie.this.type = {
    withPrefix(elem._1).value = Some(elem._2)
    this
  }

  override def subtractOne(key: K): CandidateTrie.this.type = {
    key.headOption match {
      case None =>
        // TODO: delete also all suffix refs? do the deletion in the parent and also delete the node?
        value = None
      case Some(head) =>
        suffixes.get(head).flatMap(_.remove(key.tail))
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

  override def updateWith(key: K)(remappingFunction: Option[V] => Option[V]): Option[V] = {
    val trie = withPrefix(key) // returns correct node for key
    val newValue = remappingFunction(trie.value)
    trie.value = newValue
    newValue
  }

  def updateIfDefinedWith(key: K)(remappingFunction: V => V): Option[V] = {
    val trie = withPrefix(key)
    val newValue = trie.value.map(remappingFunction)
    trie.value = newValue
    newValue
  }

  // Overloading of transformation methods that should return a CandidateTrie
  def map[W](f: ((K, V)) => (K, W)): CandidateTrie[W] =
    strictOptimizedMap(CandidateTrie.newBuilder, f)

  def flatMap[W](f: ((K, V)) => IterableOnce[(K, W)]): CandidateTrie[W] =
    strictOptimizedFlatMap(CandidateTrie.newBuilder, f)

  // Override `concat` and `empty` methods to refine their return type
  override def concat[W >: V](suffix: IterableOnce[(K, W)]): CandidateTrie[W] =
    strictOptimizedConcat(suffix, CandidateTrie.newBuilder)

  override def empty: CandidateTrie[V] = new CandidateTrie

  // Members declared in scala.collection.mutable.Clearable
  override def clear(): Unit = {
    suffixes.clear()
    value = None
  }

  // Members declared in scala.collection.IterableOps
  override protected def fromSpecific(coll: IterableOnce[(K, V)]): CandidateTrie[V] =
    CandidateTrie.from[V](coll)

  override protected def newSpecificBuilder: mutable.Builder[(K, V), CandidateTrie[V]] =
    CandidateTrie.newBuilder

  override def className = "CandidateTrie"
}


object CandidateTrie {

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

  def empty[V]: CandidateTrie[V] = new CandidateTrie[V]

  def apply[V](elems: (K, V)*): CandidateTrie[V] = from(elems)

  def from[V](it: IterableOnce[(K, V)]): CandidateTrie[V] = it match {
    case alreadyPrefixMap: CandidateTrie[V] => alreadyPrefixMap
    case _ => (newBuilder[V] ++= it).result()
  }

  def newBuilder[V]: mutable.Builder[(K, V), CandidateTrie[V]] =
    new mutable.GrowableBuilder[(K, V), CandidateTrie[V]](empty)

  implicit def toFactory[V](self: this.type): Factory[(K, V), CandidateTrie[V]] =
    new Factory[(K, V), CandidateTrie[V]] {
      override def fromSpecific(it: IterableOnce[(K, V)]): CandidateTrie[V] = self.from(it)

      override def newBuilder: mutable.Builder[(K, V), CandidateTrie[V]] = self.newBuilder
    }
}
