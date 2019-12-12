package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.CandidateState.{NewSplitCandidates, NewSwapCandidates}
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

  def createFromDelta(delta: Delta): CandidateState = delta match {
    case NewSplitCandidates(newSplitCandidates) => CandidateState(splitCandidates = newSplitCandidates)
    case NewSwapCandidates(newSwapCandidates) => CandidateState(swapCandidates = newSwapCandidates)
  }

  def createFromDeltas(deltas: Iterable[Delta]): CandidateState = CandidateState().updatedAll(deltas)

  sealed trait Delta
  final case class NewSplitCandidates(splitCandidates: CandidateSet) extends Delta
  final case class NewSwapCandidates(swapCandidates: Seq[(Int, Int)]) extends Delta

}

case class CandidateState(
    splitCandidates: CandidateSet = CandidateSet.empty,
    swapCandidates: Seq[(Int, Int)] = Seq.empty,
    splitChecked: Boolean = false,
    swapChecked: Boolean = false
) {

  def updatedAll(deltas: Iterable[CandidateState.Delta]): CandidateState =
    deltas.foldLeft(this) { case (state, delta) =>
      state.updated(delta)
    }

  def updated(delta: CandidateState.Delta): CandidateState = delta match {
    // only consider updates if we did not yet check the candidates (otherwise the update is obsolete
    case NewSplitCandidates(newSplitCandidates) if !this.splitChecked => this.copy(
      splitCandidates = newSplitCandidates
    )
    case NewSwapCandidates(newSwapCandidates) if !this.swapChecked => this.copy(
      swapCandidates = newSwapCandidates
    )
    case _ => this
  }
}
