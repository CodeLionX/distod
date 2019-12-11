package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.types.CandidateSet


object CandidateState {

  def forL0(splitCandidates: CandidateSet): CandidateState = CandidateState(
    splitCandidates,
    swapCandidates = Seq.empty,
    // we do not need to check for splits and swaps in level 0 (empty set)
    splitChecked = true,
    swapChecked = true
  )

  def forL1(splitCandidates: CandidateSet): CandidateState = CandidateState(
    splitCandidates,
    swapCandidates = Seq.empty,
    swapChecked = true // we do not need to check for swaps in level 1 (single attribute nodes)
  )
}

case class CandidateState(
    splitCandidates: CandidateSet = CandidateSet.empty,
    swapCandidates: Seq[(Int, Int)] = Seq.empty,
    splitChecked: Boolean = false,
    swapChecked: Boolean = false
)
