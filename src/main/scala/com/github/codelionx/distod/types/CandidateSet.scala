package com.github.codelionx.distod.types

import scala.collection.immutable.BitSet


object CandidateSet {

  def empty: CandidateSet = CandidateSet(BitSet.empty)

  def apply(candidates: Int*): CandidateSet = CandidateSet(BitSet(candidates: _*))
}

case class CandidateSet(x: BitSet) {

  def predecessors: Seq[CandidateSet] = x.iterator.map(elem =>
    CandidateSet(x - elem)
  ).toSeq

  def size: Int = x.size

  override def toString: String = s"CandidateSet(${x.mkString(", ")})"
}
