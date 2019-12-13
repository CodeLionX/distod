package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.types.CandidateSet


object NodeStateFilter {

  def createWithState(state: Map[CandidateSet, CandidateState], queue: WorkQueue): NodeStateFilter =
    new NodeStateFilter(state, queue)
}


class NodeStateFilter(state: Map[CandidateSet, CandidateState], queue: WorkQueue) {

  implicit class NotInQueueAndStateFilterable(val nodes: Iterable[CandidateSet]) {

    def notInWorkQueue(tpe: JobType.JobType): Iterable[CandidateSet] =
      nodes.filterNot(node => queue.contains(node -> tpe))

    def notAlreadyChecked(tpe: JobType.JobType): Iterable[CandidateSet] =
      nodes.filterNot(node =>
        state.get(node).fold(false)(s => tpe match {
          case JobType.Split => s.splitChecked
          case JobType.Swap => s.swapChecked
          case JobType.Generation => false
        })
      )
  }

  def computableSplitNodes(potentialNodes: Iterable[CandidateSet]): Iterable[CandidateSet] =
    potentialNodes
      .notInWorkQueue(JobType.Split)
      .notAlreadyChecked(JobType.Split)
      .filter { node =>
        val predecessors = node.predecessors
        predecessors.forall(id => state.get(id).fold(false)(_.splitChecked))
      }

  def computableSwapNodes(potentialNodes: Iterable[CandidateSet]): Iterable[CandidateSet] =
    potentialNodes
      .notInWorkQueue(JobType.Swap)
      .notAlreadyChecked(JobType.Swap)
      .filter { node =>
        val predecessors = node.predecessors
        predecessors.forall(id =>
          // TODO: optimize (we do not need all predecessor splits for each candidate)
          state.get(id).fold(false)(s => s.splitChecked && s.swapChecked))
      }
}