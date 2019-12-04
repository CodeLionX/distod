package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.Master.{CandidateState, JobType}
import com.github.codelionx.distod.types.CandidateSet

import scala.collection.immutable.Queue


trait CandidateGeneration {

  def generateNewCandidates(
      attributes: Seq[Int],
      state: Map[CandidateSet, CandidateState],
      currentWorkQueue: Queue[(CandidateSet, JobType.JobType)],
      pending: Set[(CandidateSet, JobType.JobType)],
      updatedCandidate: CandidateSet
  ): (Map[CandidateSet, CandidateState], Seq[(CandidateSet, JobType.JobType)]) = {

    val currentNodeState = state(updatedCandidate)
    // node pruning
    if (currentNodeState.splitCandidates.isEmpty && currentNodeState.swapCandidates.isEmpty) {
      // no valid descending candidates!
      (state, Seq.empty)
    } else {
      // optimization: Usage of [[View]]s prevents the materialization of temporary collections and speeds up the
      // iterations on larger collections.
      val potentialNewNodes = updatedCandidate
        .successors(attributes.toSet)
        .view
      val newNodesSize = updatedCandidate.size + 1

      // filtered candidates --> computable nodes
      val nodeFilter = NodeStateFilter.createWithState(state, currentWorkQueue, pending)
      val newSplitNodes = nodeFilter.computableSplitNodes(potentialNewNodes)
      val newSwapNodes = nodeFilter.computableSwapNodes(potentialNewNodes)

      // update state
      val splitUpdatedState = updateStateForSplitNodes(newSplitNodes, state)
      val swapUpdatedState = updateStateForSwapNodes(newSwapNodes, newNodesSize, splitUpdatedState)

      val newJobs = (
        newSplitNodes.map(id => id -> JobType.Split)
          ++ newSwapNodes.map(id => id -> JobType.Swap)
        ).toSeq
      (swapUpdatedState, newJobs)
    }
  }

  private def updateStateForSplitNodes(
      newNodes: Iterable[CandidateSet], state: Map[CandidateSet, CandidateState]
  ): Map[CandidateSet, CandidateState] =
    newNodes.foldLeft(state) { case (acc, id) =>
      val predecessorSplitCandidates = id.predecessors.map(state(_).splitCandidates)
      val newSplitCandidates = predecessorSplitCandidates.reduce(_ intersect _)
      val updatedNodeState = state.get(id) match {
        case Some(s) =>
          s.copy(
            splitCandidates = newSplitCandidates,
            splitChecked = false
          )
        case None => CandidateState(
          splitCandidates = newSplitCandidates,
          swapCandidates = Seq.empty
        )
      }
      acc.updated(id, updatedNodeState)
    }

  private def updateStateForSwapNodes(
      newNodes: Iterable[CandidateSet],
      newNodesSize: Int,
      state: Map[CandidateSet, CandidateState]
  ): Map[CandidateSet, CandidateState] = {
    def filterBasedOnSplits(id: CandidateSet, candidates: Seq[(Int, Int)]): Seq[(Int, Int)] = {
      candidates.filter { case (a, b) =>
        state.get(id - a).fold(false)(s => s.splitCandidates.contains(b)) &&
          state.get(id - b).fold(false)(s => s.splitCandidates.contains(a))
      }
    }

    newNodes.foldLeft(state) { case (acc, id) =>
      val newSwapCandidates =
        if (newNodesSize == 2) {
          // every node (with size 2) only has one candidate (= itself)
          val attribute1 = id.toIndexedSeq(0)
          val attribute2 = id.toIndexedSeq(1)
          filterBasedOnSplits(id, Seq(attribute1 -> attribute2))

        } else {
          // every new node's potential swap candidates are the union of its predecessors
          val predecessorSwapCandidates = id.predecessors.map(state(_).swapCandidates)
          val potentialSwapCandidates = predecessorSwapCandidates.reduce(_ ++ _).distinct
          val updatedPotentialSwapCandidates = potentialSwapCandidates.filter { case (a, b) =>
            val referenceSet = id - a - b
            referenceSet.forall(attribute => state(id - attribute).swapCandidates.contains(a -> b))
          }
          filterBasedOnSplits(id, updatedPotentialSwapCandidates)
        }

      val updatedNodeState = state.get(id) match {
        case Some(s) =>
          s.copy(
            swapCandidates = newSwapCandidates,
            swapChecked = false
          )
        case None => CandidateState(
          splitCandidates = CandidateSet.empty,
          swapCandidates = newSwapCandidates
        )
      }

      acc.updated(id, updatedNodeState)
    }
  }
}
