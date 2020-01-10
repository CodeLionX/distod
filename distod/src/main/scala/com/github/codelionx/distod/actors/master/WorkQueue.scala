package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.types.CandidateSet

import scala.annotation.tailrec
import scala.collection.immutable.Queue


object WorkQueue {

  type Item = (CandidateSet, JobType.JobType)

  val empty: WorkQueue = new WorkQueue(
    Queue.empty,
    Set.empty,
    Set.empty,
    Set.empty
  )

  def apply(initialItems: Item*): WorkQueue = from(initialItems)

  def from(candidates: IterableOnce[Item]): WorkQueue =
    new WorkQueue(Queue.from(candidates), Set.from(candidates), Set.empty, Set.empty)
}


// TODO: refactor as case class (more efficient and easier copy)
class WorkQueue private(
    workQueue: Queue[WorkQueue.Item],
    work: Set[WorkQueue.Item],
    pending: Set[WorkQueue.Item],
    pendingGeneration: Set[WorkQueue.Item]
) {

  /**
   * Tests whether the work queue is empty (this includes pending responses and the actual work queue).
   *
   * @return `true` if the work queue contains no elements, `false` otherwise.
   */
  def isEmpty: Boolean = work.isEmpty && pending.isEmpty && pendingGeneration.isEmpty

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
  def hasPending: Boolean = pending.nonEmpty || pendingGeneration.nonEmpty

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
    val newWork = work - item
    val newPending = pending + item
    (item, new WorkQueue(newQueue, newWork, newPending, pendingGeneration))
  }

  /**
   * Jumps over all items in the queue that were already removed (from the work set).
   * This is a performance optimization (compared to actually removing them from the queue)!
   */
  @tailrec
  private final def internalDequeue(queue: Queue[(CandidateSet, JobType.JobType)]): (WorkQueue.Item, Queue[(CandidateSet, JobType.JobType)]) = {
    val (item, newQueue) = queue.dequeue
    if(work.contains(item)) {
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
  def enqueue(item: WorkQueue.Item): WorkQueue = {
    val newWorkQueue = workQueue.enqueue(item)
    val newWork = work + item
    new WorkQueue(newWorkQueue, newWork, pending, pendingGeneration)
  }

  /**
   * Creates a new queue with all elements provided by an `Iterable` object added at the end of the old queue. The
   * pending set is not changed. The elements are appended in the order they are given out by the iterator.
   *
   * @param  items an iterable object
   */
  def enqueueAll(items: Iterable[WorkQueue.Item]): WorkQueue = {
    val newWorkQueue = workQueue.enqueueAll(items)
    val newWork = work ++ items
    new WorkQueue(newWorkQueue, newWork, pending, pendingGeneration)
  }

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
  def removePending(item: WorkQueue.Item): WorkQueue = {
    val newPending = pending - item
    new WorkQueue(workQueue, work, newPending, pendingGeneration)
  }

  def addPendingGenerationAll(items: Iterable[WorkQueue.Item]): WorkQueue = {
    val newPendingGeneration = pendingGeneration ++ items
    new WorkQueue(workQueue, work, pending, newPendingGeneration)
  }

  @inline def addPendingGeneration(key: CandidateSet, tpe: JobType.JobType): WorkQueue =
    addPendingGeneration(key -> tpe)

  def addPendingGeneration(item: WorkQueue.Item): WorkQueue = {
    val newPendingGeneration = pendingGeneration + item
    new WorkQueue(workQueue, work, pending, newPendingGeneration)
  }

  @inline def removePendingGeneration(key: CandidateSet, tpe: JobType.JobType): WorkQueue =
    removePendingGeneration(key -> tpe)

  def removePendingGeneration(item: WorkQueue.Item): WorkQueue = {
    val newPendingGeneration = pendingGeneration - item
    new WorkQueue(workQueue, work, pending, newPendingGeneration)
  }

  /**
   * Tests whether the `item` is contained either in the work queue or the pending set (includes the pending generation
   * set).
   * This is an improved inclusion test that was optimizes for performance using a set (`O(1)`).
   */
  def contains(item: WorkQueue.Item): Boolean = work.contains(item) || pending.contains(item) || pendingGeneration.contains(item)

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
   * Tests wether the `item` is contained in the pending generation set.
   *
   * @see [[com.github.codelionx.distod.actors.master.WorkQueue#contains]]
   */
  def containsPendingGeneration(item: WorkQueue.Item): Boolean = pendingGeneration.contains(item)

  /**
   * Removes all entries from the queue that contain one of the candidates. Does not touch the pending queues.
   */
  def removeAll(candidates: Set[CandidateSet]): WorkQueue = {
    val jobs: Set[(CandidateSet, JobType.JobType)] = candidates.flatMap(c => Seq(c -> JobType.Split, c -> JobType.Swap))
    val newWork = work -- jobs
    new WorkQueue(workQueue, newWork, pending, pendingGeneration)
  }

  override def toString: String =
    s"""WorkQueue(
       |    queue=${workQueue.size}
       |    work=${work.size}
       |    pending=${pending.size}
       |    generationPending=${pendingGeneration.size}
       |)""".stripMargin
}
