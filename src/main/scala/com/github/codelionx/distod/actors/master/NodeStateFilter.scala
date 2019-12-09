package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.Master.{CandidateState, JobType}
import com.github.codelionx.distod.types.CandidateSet

import scala.collection.immutable.Queue


object NodeStateFilter {

  def createWithState(
      state: Map[CandidateSet, CandidateState],
      queue: Queue[(CandidateSet, JobType.JobType)],
      pending: Set[(CandidateSet, JobType.JobType)]
  ): NodeStateFilter = new NodeStateFilter(state, queue, pending)
}


class NodeStateFilter(
    state: Map[CandidateSet, CandidateState],
    queue: Queue[(CandidateSet, JobType.JobType)],
    pending: Set[(CandidateSet, JobType.JobType)]
) {

  implicit class NotInQueueAndPendingFilterable(val nodes: Iterable[CandidateSet]) {

    def notInQueue(tpe: JobType.JobType): Iterable[CandidateSet] =
      nodes.filterNot(node => queue.contains(node -> tpe))

    def notInPending(tpe: JobType.JobType): Iterable[CandidateSet] =
      nodes.filterNot(node => pending.contains(node -> tpe))

    def notAlreadyChecked(tpe: JobType.JobType): Iterable[CandidateSet] =
      nodes.filterNot(node =>
        state.get(node).fold(false)(s => tpe match {
          case JobType.Split => s.splitChecked
          case JobType.Swap => s.swapChecked
        })
      )
  }

  def computableSplitNodes(potentialNodes: Iterable[CandidateSet]): Iterable[CandidateSet] =
    potentialNodes
      .notInQueue(JobType.Split)
      .notInPending(JobType.Split)
      .notAlreadyChecked(JobType.Split)
      .filter { node =>
        val predecessors = node.predecessors
        predecessors.forall(id => state.get(id).fold(false)(_.splitChecked))
      }

  def computableSwapNodes(potentialNodes: Iterable[CandidateSet]): Iterable[CandidateSet] =
    potentialNodes
      .notInQueue(JobType.Swap)
      .notInPending(JobType.Swap)
      .notAlreadyChecked(JobType.Swap)
      .filter { node =>
        val predecessors = node.predecessors
        predecessors.forall(id =>
          // TODO: optimize (we do not need all predecessor splits for each candidate)
          state.get(id).fold(false)(s => s.splitChecked && s.swapChecked))
      }
}