package com.github.codelionx.distod.types

import scala.collection.{mutable, SpecificIterableFactory}
import scala.collection.immutable.{BitSet, SortedSet, SortedSetOps, StrictOptimizedSortedSetOps}


object CandidateSet extends SpecificIterableFactory[Int, CandidateSet] {

  def empty: CandidateSet = new CandidateSet(BitSet.empty, 0)

  def from(candidates: Int*): CandidateSet = fromSpecific(candidates)

  def fromSpecific(candidates: IterableOnce[Int]): CandidateSet =
    apply(BitSet.fromSpecific(candidates))

  def fromBitMask(elems: Array[Long]): CandidateSet = apply(BitSet.fromBitMask(elems))

  def apply(bitset: BitSet): CandidateSet = new CandidateSet(bitset, bitset.size)

  override def newBuilder: mutable.Builder[Int, CandidateSet] =
    new mutable.Builder[Int, CandidateSet] {
      private val bitSetBuilder = BitSet.newBuilder
      private var size = 0

      override def clear(): Unit = {
        size = 0
        bitSetBuilder.clear()
      }

      override def result(): CandidateSet = new CandidateSet(bitSetBuilder.result(), size)

      override def addOne(elem: Int): this.type = {
        bitSetBuilder.addOne(elem)
        size += 1
        this
      }
    }

}


/**
 * Represents a set of candidates (column Ids). It uses a [[scala.collection.immutable.BitSet]] as underlying data
 * structure that allows space-efficient storage and efficient set-inclusion testing and set-combination operations.
 *
 * This wrapper caches the size of the underlying BitSet (set bits) to allow fast comparison and size requests.
 *
 * @see [[scala.collection.immutable.BitSet]]
 */
class CandidateSet(private val _underlying: BitSet, private val _size: Int)
  extends SortedSet[Int]
    with SortedSetOps[Int, SortedSet, CandidateSet]
    with StrictOptimizedSortedSetOps[Int, SortedSet, CandidateSet] {

  private def factory: SpecificIterableFactory[Int, CandidateSet] = CandidateSet

  protected override def fromSpecific(coll: IterableOnce[Int]): CandidateSet = factory.fromSpecific(coll)

  protected override def newSpecificBuilder: mutable.Builder[Int, CandidateSet] = factory.newBuilder

  override def empty: CandidateSet = factory.empty

  override def iteratorFrom(start: Int): Iterator[Int] = _underlying.iteratorFrom(start)

  override def incl(elem: Int): CandidateSet =
    if (contains(elem)) this
    else new CandidateSet(_underlying.incl(elem), _size + 1)

  override def excl(elem: Int): CandidateSet =
    if (!contains(elem)) this
    else new CandidateSet(_underlying.excl(elem), _size - 1)

  override def contains(elem: Int): Boolean = _underlying.contains(elem)

  override def iterator: Iterator[Int] = _underlying.iterator

  override def ordering: Ordering[Int] = _underlying.ordering

  override def rangeImpl(from: Option[Int], until: Option[Int]): CandidateSet = {
    val bitset = _underlying.rangeImpl(from, until)
    new CandidateSet(bitset, bitset.size)
  }

  override def size: Int = _size

  /**
   * Computes the predecessors of this CandidateSet. E.g. for CandidateSet(0, 1, 2), the predecessors are:
   * - CandidateSet(0, 1)
   * - CandidateSet(1, 2)
   * - CandidateSet(0, 2)
   *
   * @return A new sequence of the preceding candidate sets.
   */
  def predecessors: Set[CandidateSet] = _underlying.unsorted.map(elem =>
    new CandidateSet(_underlying - elem, _size - 1)
  )

  /**
   * Computes the successors of this CandidateSet using the specified attributes. E.g. for CandidateSet(1, 2) and
   * `allAttributes = Seq(0, 1, 2, 3)`, the successors are:
   * - CandidateSet(0, 1, 2) and
   * - CandidateSet(1, 2, 3).
   *
   * @param allAttributes all attributes to be considered in the computation of the successors in the lattice
   */
  def successors(allAttributes: Set[Int]): Set[CandidateSet] = successors(CandidateSet.fromSpecific(allAttributes))

  /**
   * Computes the successors of this CandidateSet using the specified attribute set. E.g. for CandidateSet(1, 2) and
   * `allAttributes = Seq(0, 1, 2, 3)`, the successors are:
   * - CandidateSet(0, 1, 2) and
   * - CandidateSet(1, 2, 3).
   *
   * @param allAttributes set over all attributes to be considered in the computation of the successors in the lattice
   */
  def successors(allAttributes: CandidateSet): Set[CandidateSet] =
    (allAttributes._underlying diff this._underlying)
      .unsorted
      .map { attribute =>
        new CandidateSet(this._underlying + attribute, this._size + 1)
      }


  override def toString(): String = s"CandidateSet(${_underlying.mkString(", ")})"
}
