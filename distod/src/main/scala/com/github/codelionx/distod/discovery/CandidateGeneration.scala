package com.github.codelionx.distod.discovery

import com.github.codelionx.distod.actors.master.{CandidateState, JobType, NodeStateFilter, WorkQueue}
import com.github.codelionx.distod.types.CandidateSet


trait CandidateGeneration {

  def generateNewCandidates(
      attributes: Seq[Int],
      state: Map[CandidateSet, CandidateState],
      currentWorkQueue: WorkQueue,
      updatedCandidate: CandidateSet
  ): (Iterable[(CandidateSet, JobType.JobType)], Iterable[(CandidateSet, CandidateState.Delta)]) = {

    val currentNodeState = state(updatedCandidate)
    // node pruning
    if (currentNodeState.splitCandidates.isEmpty && currentNodeState.swapCandidates.isEmpty) {
      // no valid descending candidates!
      (Seq.empty, Map.empty)
    } else {
      val potentialNewNodes = updatedCandidate.successors(attributes.toSet)
      val newNodesSize = updatedCandidate.size + 1

      // filtered candidates --> computable nodes
      val nodeFilter = NodeStateFilter.createWithState(state, currentWorkQueue)
      val newSplitNodes = nodeFilter.computableSplitNodes(potentialNewNodes)
      val newSwapNodes = nodeFilter.computableSwapNodes(potentialNewNodes)

      // state updates
      val splitUpdates = updateStateForSplitNodes(newSplitNodes, state)
      val swapUpdates = updateStateForSwapNodes(newSwapNodes, newNodesSize, state)

      val newJobs = (
        newSplitNodes.map(id => id -> JobType.Split)
          ++ newSwapNodes.map(id => id -> JobType.Swap)
        )
      val updates = splitUpdates ++ swapUpdates
      (newJobs, updates)
    }
  }

  private def updateStateForSplitNodes(
      newNodes: Iterable[CandidateSet], state: Map[CandidateSet, CandidateState]
  ): Iterable[(CandidateSet, CandidateState.Delta)] =
    newNodes.map { id =>
      val predecessorSplitCandidates = id.predecessors.map(state(_).splitCandidates)
      val newSplitCandidates = predecessorSplitCandidates.reduce(_ intersect _)
      id -> CandidateState.NewSplitCandidates(newSplitCandidates)
    }

  private def updateStateForSwapNodes(
      newNodes: Iterable[CandidateSet],
      newNodesSize: Int,
      state: Map[CandidateSet, CandidateState]
  ): Iterable[(CandidateSet, CandidateState.Delta)] = {
    def filterBasedOnSplits(id: CandidateSet, candidates: Seq[(Int, Int)]): Seq[(Int, Int)] = {
      candidates.filter { case (a, b) =>
        state.get(id - a).fold(false)(s => s.splitCandidates.contains(b)) &&
          state.get(id - b).fold(false)(s => s.splitCandidates.contains(a))
      }
    }

    newNodes.map { id =>
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
      id -> CandidateState.NewSwapCandidates(newSwapCandidates)
    }
  }
}
