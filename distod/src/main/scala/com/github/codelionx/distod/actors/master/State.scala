package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.types.CandidateSet

import scala.collection.{mutable, StrictOptimizedIterableOps}
import scala.collection.immutable.{AbstractMap, Iterable, Map}


object State {

  def empty[V]: State[V] = new State(IndexedSeq.empty)

  def apply[V](elems: (CandidateSet, V)*): State[V] = fromSpecific(elems)

  def fromSpecific[V](coll: IterableOnce[(CandidateSet, V)]): State[V] = {
    val b = newSpecificBuilder[V]
    b.addAll(coll)
    b.result()
  }

  def newSpecificBuilder[V]: mutable.Builder[(CandidateSet, V), State[V]] = new StateBuilder[V]()

  private[State] class StateBuilder[V] private[State](
      startState: IndexedSeq[Map[CandidateSet, V]] = IndexedSeq(Map.empty[CandidateSet, V])
  ) extends mutable.Builder[(CandidateSet, V), State[V]] {

    private var internalState: IndexedSeq[Map[CandidateSet, V]] = startState

    override def addOne(elem: (CandidateSet, V)): this.type = {
      val (key, value) = elem
      // fill seq up with empty maps (prevents IndexOutOfBoundsException)
      var preparedState = internalState
      while (preparedState.size <= key.size) {
        preparedState :+= Map.empty
      }
      // select correct map and update it with new mapping
      val map = preparedState(key.size).updated(key, value)
      // update builder state
      internalState = preparedState.updated(key.size, map)
      this
    }

    override def clear(): Unit = {
      internalState = IndexedSeq(Map.empty)
    }

    override def result(): State[V] = new State(internalState)
  }
}


class State[V] private(levels: IndexedSeq[Map[CandidateSet, V]])
  extends AbstractMap[CandidateSet, V]
    with StrictOptimizedIterableOps[(CandidateSet, V), Iterable, State[V]] {

  private def factory: State.type = State

  override def fromSpecific(coll: IterableOnce[(CandidateSet, V)]): State[V] = factory.fromSpecific(coll)

  override def newSpecificBuilder: mutable.Builder[(CandidateSet, V), State[V]] = factory.newSpecificBuilder

  override def empty: State[V] = factory.empty

//  @inline override def -(key: CandidateSet): State[V] = removed(key)

  override def removed(key: CandidateSet): State[V] = {
    val selectedMap = levels(key.size)
    val updatedMap = selectedMap.removed(key)
    new State(levels.updated(key.size, updatedMap))
  }

  @inline override def +[V1 >: V](kv: (CandidateSet, V1)): State[V1] = updated(kv._1, kv._2)

  override def updated[V1 >: V](key: CandidateSet, value: V1): State[V1] = {
    val b = new State.StateBuilder[V1](levels)
    b.addOne(key -> value)
    b.result()
  }

  override def get(key: CandidateSet): Option[V] =
    if (key.size >= levels.size)
      None
    else
      levels.apply(key.size).get(key)

  override def iterator: Iterator[(CandidateSet, V)] = levels.iterator.flatMap(_.iterator)
}