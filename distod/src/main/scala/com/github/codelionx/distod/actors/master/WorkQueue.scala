package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.types.CandidateSet

import scala.annotation.tailrec
import scala.collection.immutable.Queue


object WorkQueue {

  type Item = (CandidateSet, JobType.JobType)

  val empty: WorkQueue = WorkQueue(
    Queue.empty,
    Set.empty,
    Set.empty
  )

  def from(initialItems: Item*): WorkQueue = from(initialItems)

  def from(candidates: IterableOnce[Item]): WorkQueue =
    WorkQueue(Queue.from(candidates), Set.from(candidates), Set.empty)
}


case class WorkQueue private(
    workQueue: Queue[WorkQueue.Item],
    work: Set[WorkQueue.Item],
    pending: Set[WorkQueue.Item]
) {

  def size: Int = sizeWork + sizePending

  def sizeWork: Int = work.size

  def sizePending: Int = pending.size

  /**
   * Tests whether the work queue is empty (this includes pending responses and the actual work queue).
   *
   * @return `true` if the work queue contains no elements, `false` otherwise.
   */
  def isEmpty: Boolean = work.isEmpty && pending.isEmpty

  /**
   * Tests whether the work queue is not empty (this includes pending responses and the actual work queue).
   *
   * @return `true` if the work queue or the pending set contains at least one element, `false` otherwise.
   */
  def nonEmpty: Boolean = !isEmpty

  /**
   * Tests whether there is still work in the queue.
   */
  def hasWork: Boolean = work.nonEmpty

  /**
   * Tests whether there is no work in the queue.
   */
  def noWork: Boolean = !hasWork

  /**
   * Tests whether there are still pending responses.
   */
  def hasPending: Boolean = pending.nonEmpty

  /**
   * Tests whether there are no pending responses.
   */
  def noPending: Boolean = !hasPending

  /**
   * Dequeues the first item from the work queue and puts it into the pending set.
   *
   * @throws java.util.NoSuchElementException when the queue is empty
   * @return a tuple with the first element in the queue, and the new queue with the element put into the pending set
   */
  def dequeue(): (WorkQueue.Item, WorkQueue) = {
    val (item, newQueue) = internalDequeue(workQueue)
    val newObj = copy(
      workQueue = newQueue,
      work = work - item,
      pending = pending + item
    )
    (item, newObj)
  }

  /**
   * Jumps over all items in the queue that were already removed (from the work set).
   * This is a performance optimization (compared to actually removing them from the queue)!
   */
  @tailrec
  private final def internalDequeue(
      queue: Queue[(CandidateSet, JobType.JobType)]
  ): (WorkQueue.Item, Queue[(CandidateSet, JobType.JobType)]) = {
    val (item, newQueue) = queue.dequeue
    if (work.contains(item)) {
      (item, newQueue)
    } else {
      internalDequeue(newQueue)
    }
  }

  /**
   * Creates a new queue with element added at the end of the old queue. The pending set is not changed.
   *
   * @param  item the element to insert
   */
  def enqueue(item: WorkQueue.Item): WorkQueue =
    copy(
      workQueue = workQueue.enqueue(item),
      work = work + item
    )

  /**
   * Creates a new queue with all elements provided by an `Iterable` object added at the end of the old queue. The
   * pending set is not changed. The elements are appended in the order they are given out by the iterator.
   *
   * @param  items an iterable object
   */
  def enqueueAll(items: Iterable[WorkQueue.Item]): WorkQueue =
    copy(
      workQueue = workQueue.enqueueAll(items),
      work = work ++ items
    )

  /**
   * Alias to `removePending(item: WorkQueue.Item)`
   */
  @inline def removePending(key: CandidateSet, tpe: JobType.JobType): WorkQueue = removePending(key -> tpe)

  /**
   * Creates a new queue with a given element removed from the pending set. The actual work queue is not changed.
   *
   * @param item the element to be removed
   * @return a new queue that contains all elements of the current pending set but that does not contain `elem`.
   */
  def removePending(item: WorkQueue.Item): WorkQueue =
    copy(
      pending = pending - item
    )

  /**
   * Tests whether the `item` is contained either in the work queue or the pending set.
   * This is an improved inclusion test that was optimizes for performance using a set (`O(1)`).
   */
  def contains(item: WorkQueue.Item): Boolean = work.contains(item) || pending.contains(item)

  /**
   * Tests whether the `item` is contained in the actual work queue.
   *
   * @see [[com.github.codelionx.distod.actors.master.WorkQueue#contains]]
   */
  def containsWork(item: WorkQueue.Item): Boolean = work.contains(item)

  /**
   * Tests whether the `item` is contained in the pending set.
   *
   * @see [[com.github.codelionx.distod.actors.master.WorkQueue#contains]]
   */
  def containsPending(item: WorkQueue.Item): Boolean = pending.contains(item)

  /**
   * Removes all entries from the queue that contain one of the candidates. Does not touch the pending queues.
   */
  def removeAll(candidates: Set[CandidateSet]): WorkQueue = {
    val jobs: Set[(CandidateSet, JobType.JobType)] = candidates.flatMap(c => Seq(c -> JobType.Split, c -> JobType.Swap))
    copy(
      work = work -- jobs
    )
  }

  override def toString: String =
    "WorkQueue(" +
      s"queue=${workQueue.size}," +
      s"work=${work.size}," +
      s"pending=${pending.size}" +
      ")"
}
