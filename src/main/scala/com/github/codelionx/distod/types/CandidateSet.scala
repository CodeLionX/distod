package com.github.codelionx.distod.types

import scala.collection.{mutable, SpecificIterableFactory}
import scala.collection.immutable.{BitSet, SortedSet, SortedSetOps, StrictOptimizedSortedSetOps}


object CandidateSet extends SpecificIterableFactory[Int, CandidateSet] {

  def empty: CandidateSet = new CandidateSet(BitSet.empty)

  def from(candidates: Int*): CandidateSet = fromSpecific(candidates)

  def fromSpecific(candidates: IterableOnce[Int]): CandidateSet =
    apply(BitSet.fromSpecific(candidates))

  def fromBitMask(elems: Array[Long]): CandidateSet = apply(BitSet.fromBitMask(elems))

  def apply(bitset: BitSet): CandidateSet = new CandidateSet(bitset)

  override def newBuilder: mutable.Builder[Int, CandidateSet] =
    new mutable.Builder[Int, CandidateSet] {
      private val bitSetBuilder = BitSet.newBuilder

      override def clear(): Unit = bitSetBuilder.clear()

      override def result(): CandidateSet = apply(bitSetBuilder.result())

      override def addOne(elem: Int): this.type = {
        bitSetBuilder.addOne(elem)
        this
      }
    }
}


class CandidateSet(_underlying: BitSet)
  extends SortedSet[Int]
    with SortedSetOps[Int, SortedSet, CandidateSet]
    with StrictOptimizedSortedSetOps[Int, SortedSet, CandidateSet] {

  def factory: SpecificIterableFactory[Int, CandidateSet] = CandidateSet

  protected override def fromSpecific(coll: IterableOnce[Int]): CandidateSet = factory.fromSpecific(coll)

  protected override def newSpecificBuilder: mutable.Builder[Int, CandidateSet] = factory.newBuilder

  override def empty: CandidateSet = factory.empty

  override def iteratorFrom(start: Int): Iterator[Int] = _underlying.iteratorFrom(start)

  override def incl(elem: Int): CandidateSet =
    if (contains(elem)) this
    else new CandidateSet(_underlying.incl(elem))

  override def excl(elem: Int): CandidateSet =
    if (!contains(elem)) this
    else new CandidateSet(_underlying.excl(elem))

  override def contains(elem: Int): Boolean = _underlying.contains(elem)

  override def iterator: Iterator[Int] = _underlying.iterator

  override def ordering: Ordering[Int] = _underlying.ordering

  override def rangeImpl(from: Option[Int], until: Option[Int]): CandidateSet =
    new CandidateSet(_underlying.rangeImpl(from, until))

  def predecessors: Seq[CandidateSet] = _underlying.unsorted.map(elem =>
    new CandidateSet(_underlying - elem)
  ).toSeq
}
