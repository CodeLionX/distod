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
    // node pruning (again), see master
    if (currentNodeState.splitCandidates.isEmpty && currentNodeState.swapCandidates.isEmpty) {
      // no valid descending candidates!
      (Seq.empty, Map.empty)
    } else {
      val potentialNewNodes = updatedCandidate.successors(attributes.toSet)

      // filtered candidates --> computable nodes
      val nodeFilter = NodeStateFilter.createWithState(state, currentWorkQueue)
      val newSplitNodes = nodeFilter.computableSplitNodes(potentialNewNodes)
      val newSwapNodes = nodeFilter.computableSwapNodes(potentialNewNodes)

      // state updates
      val splitUpdates = updateStateForSplitNodes(newSplitNodes, state)
      val swapUpdates = updateStateForSwapNodes(newSwapNodes, state)

      val newJobs = (
        newSplitNodes.map(id => id -> JobType.Split)
          ++ newSwapNodes.map(id => id -> JobType.Swap)
        )
      val updates = splitUpdates ++ swapUpdates
      (newJobs, updates)
    }
  }

  def generateSplitCandidates(id: CandidateSet, state: Map[CandidateSet, CandidateState]): CandidateSet = {
    val predecessorSplitCandidates = id.predecessors.map(state(_).splitCandidates)
    predecessorSplitCandidates.reduce(_ intersect _)
  }

  def generateSwapCandidates(id: CandidateSet, state: Map[CandidateSet, CandidateState]): Seq[(Int, Int)] = {
    def filterBasedOnSplits(id: CandidateSet, candidates: Seq[(Int, Int)]): Seq[(Int, Int)] = {
      candidates.filter { case (a, b) =>
        state.get(id - a).fold(false)(s => s.splitCandidates.contains(b)) &&
          state.get(id - b).fold(false)(s => s.splitCandidates.contains(a))
      }
    }
    val newNodesSize = id.size
    if (newNodesSize == 2) {
      // every node (with size 2) only has one candidate (= itself)
      val indexedId = id.toIndexedSeq
      val attribute1 = indexedId(0)
      val attribute2 = indexedId(1)
      filterBasedOnSplits(id, Seq(attribute1 -> attribute2))

    } else {
      // every new node's potential swap candidates are the union of its predecessors
      val predecessorSwapCandidates = id.predecessors.map(state(_).swapCandidates)
      val potentialSwapCandidates = predecessorSwapCandidates.reduce(_ ++ _).distinct
      val updatedPotentialSwapCandidates = potentialSwapCandidates.filter { case (a, b) =>
        val referenceSet = id - a - b
        referenceSet.forall(attribute => state.get(id - attribute).fold(false)(_.swapCandidates.contains(a -> b)))
      }
      filterBasedOnSplits(id, updatedPotentialSwapCandidates)
    }
  }

  private def updateStateForSplitNodes(
      newNodes: Iterable[CandidateSet], state: Map[CandidateSet, CandidateState]
  ): Iterable[(CandidateSet, CandidateState.Delta)] =
    newNodes.map { id =>
      val newSplitCandidates = generateSplitCandidates(id, state)
      id -> CandidateState.NewSplitCandidates(newSplitCandidates)
    }

  private def updateStateForSwapNodes(
      newNodes: Iterable[CandidateSet],
      state: Map[CandidateSet, CandidateState]
  ): Iterable[(CandidateSet, CandidateState.Delta)] =
    newNodes.map { id =>
      val newSwapCandidates = generateSwapCandidates(id, state)
      id -> CandidateState.NewSwapCandidates(newSwapCandidates)
    }
}
