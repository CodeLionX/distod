package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.Master.JobType
import com.github.codelionx.distod.types.CandidateSet

import scala.collection.immutable.Queue


object WorkQueue {

  type Item = (CandidateSet, JobType.JobType)

  val empty: WorkQueue = new WorkQueue(
    Queue.empty,
    Set.empty,
    Set.empty
  )

  def apply(initialItems: Item*): WorkQueue = from(initialItems)

  def from(candidates: IterableOnce[Item]): WorkQueue =
    new WorkQueue(Queue.from(candidates), Set.from(candidates), Set.empty)
}


class WorkQueue private(
    workQueue: Queue[WorkQueue.Item],
    work: Set[WorkQueue.Item],
    pending: Set[WorkQueue.Item],
) {

  def isEmpty: Boolean = work.isEmpty && pending.isEmpty

  def nonEmpty: Boolean = !isEmpty

  def hasWork: Boolean = work.nonEmpty

  def noWork: Boolean = !hasWork

  def hasPending: Boolean = pending.nonEmpty

  def noPending: Boolean = !hasPending

  def dequeue(): (WorkQueue.Item, WorkQueue) = {
    val (item, newQueue) = workQueue.dequeue
    val newWork = work - item
    val newPending = pending + item
    (item, new WorkQueue(newQueue, newWork, newPending))
  }

  def enqueue(item: WorkQueue.Item): WorkQueue = {
    val newWorkQueue = workQueue.enqueue(item)
    val newWork = work + item
    new WorkQueue(newWorkQueue, newWork, pending)
  }

  def enqueueAll(items: Iterable[WorkQueue.Item]): WorkQueue = {
    val newWorkQueue = workQueue.enqueueAll(items)
    val newWork = work ++ items
    new WorkQueue(newWorkQueue, newWork, pending)
  }

  def removePending(key: CandidateSet, tpe: JobType.JobType): WorkQueue = removePending(key -> tpe)

  def removePending(item: WorkQueue.Item): WorkQueue = {
    val newPending = pending - item
    new WorkQueue(workQueue, work, newPending)
  }

  def contains(item: WorkQueue.Item): Boolean = work.contains(item) || pending.contains(item)

  def containsWork(item: WorkQueue.Item): Boolean = work.contains(item)

  def containsPending(item: WorkQueue.Item): Boolean = pending.contains(item)
}
