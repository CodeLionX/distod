package com.github.codelionx.distod.actors.master

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import com.github.codelionx.distod.actors.master.CandidateState.{NewSplitCandidates, NewSwapCandidates}
import com.github.codelionx.distod.types.CandidateSet


object CandidateState {

  def forL0(id: CandidateSet, splitCandidates: CandidateSet): CandidateState = CandidateState(
    id,
    splitCandidates,
    swapCandidates = Seq.empty,
    // we do not need to check for splits and swaps in level 0 (empty set)
    splitChecked = true,
    swapChecked = true
  )

  def forL1(id: CandidateSet, splitCandidates: CandidateSet): CandidateState = CandidateState(
    id,
    splitCandidates,
    swapCandidates = Seq.empty,
    swapChecked = true // we do not need to check for swaps in level 1 (single attribute nodes)
  )

  def createFromDelta(id: CandidateSet, delta: Delta): CandidateState = delta match {
    case NewSplitCandidates(newSplitCandidates) => CandidateState(id, splitCandidates = newSplitCandidates)
    case NewSwapCandidates(newSwapCandidates) => CandidateState(id, swapCandidates = newSwapCandidates)
  }

  def createFromDeltas(id: CandidateSet, deltas: Iterable[Delta]): CandidateState =
    CandidateState(id).updatedAll(deltas)

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[NewSplitCandidates]),
    new JsonSubTypes.Type(value = classOf[NewSwapCandidates]),
  ))
  sealed trait Delta

  @JsonTypeName("NewSplitCandidates")
  final case class NewSplitCandidates(splitCandidates: CandidateSet) extends Delta

  @JsonTypeName("NewSwapCandidates")
  final case class NewSwapCandidates(swapCandidates: Seq[(Int, Int)]) extends Delta

}

case class CandidateState(
    id: CandidateSet,
    splitCandidates: CandidateSet = CandidateSet.empty,
    swapCandidates: Seq[(Int, Int)] = Seq.empty,
    splitChecked: Boolean = false,
    swapChecked: Boolean = false,
    splitPreconditions: Int = 0,
    swapPreconditions: Int = 0
  ) {

  def isReady(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => id.size == splitPreconditions
    case JobType.Swap => id.size == swapPreconditions
    case JobType.Generation => false
  }

  def notReady(jobType: JobType.JobType): Boolean = !isReady(jobType)

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
