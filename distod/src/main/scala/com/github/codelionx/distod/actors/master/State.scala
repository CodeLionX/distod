package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.types.CandidateSet

import scala.collection.{mutable, StrictOptimizedIterableOps}
import scala.collection.immutable.{AbstractMap, Iterable, Map}


object State {

  def empty[V]: State[V] = State(IndexedSeq.empty)
}


case class State[V](levels: IndexedSeq[Map[CandidateSet, V]])
  extends AbstractMap[CandidateSet, V]
    with StrictOptimizedIterableOps[(CandidateSet, V), Iterable, State[V]] {

  override def fromSpecific(coll: IterableOnce[(CandidateSet, V)]): State[V] = ???

  override def newSpecificBuilder: mutable.Builder[(CandidateSet, V), State[V]] = ???

  override def empty: State[V] = State.empty

//  @inline override def -(key: CandidateSet): State[V] = removed(key)

  override def removed(key: CandidateSet): State[V] = {
    val selectedMap = levels(key.size)
    val updatedMap = selectedMap.removed(key)
    State(levels.updated(key.size, updatedMap))
  }

  @inline override def +[V1 >: V](kv: (CandidateSet, V1)): State[V1] = updated(kv._1, kv._2)

  override def updated[V1 >: V](key: CandidateSet, value: V1): State[V1] = {
    val newLevels =
      if (key.size >= levels.size) {
        var l = levels
        while (l.size <= key.size) {
          l :+= Map.empty
        }
        l
      } else {
        levels
      }
    val selectedMap = newLevels(key.size)
    val updatedMap = selectedMap.updated(key, value)
    State(newLevels.updated(key.size, updatedMap))
  }

  override def get(key: CandidateSet): Option[V] =
    if (key.size >= levels.size)
      None
    else
      levels.apply(key.size).get(key)

  override def iterator: Iterator[(CandidateSet, V)] = levels.iterator.flatMap(_.iterator)
}