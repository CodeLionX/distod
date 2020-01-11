package com.github.codelionx.util.largeMap

import com.github.codelionx.distod.types.CandidateSet

import scala.collection.{mutable, StrictOptimizedIterableOps}
import scala.collection.immutable.{AbstractMap, Iterable, Map}


object HashMapState {

  def empty[V]: HashMapState[V] = new HashMapState(IndexedSeq.empty)

  def apply[V](elems: (CandidateSet, V)*): HashMapState[V] = fromSpecific(elems)

  def fromSpecific[V](coll: IterableOnce[(CandidateSet, V)]): HashMapState[V] = {
    val b = newSpecificBuilder[V]
    b.addAll(coll)
    b.result()
  }

  def newSpecificBuilder[V]: mutable.Builder[(CandidateSet, V), HashMapState[V]] = new StateBuilder[V]()

  private[HashMapState] class StateBuilder[V] private[HashMapState](
      startState: IndexedSeq[Map[CandidateSet, V]] = IndexedSeq(Map.empty[CandidateSet, V])
  ) extends mutable.Builder[(CandidateSet, V), HashMapState[V]] {

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

    override def result(): HashMapState[V] = new HashMapState(internalState)
  }
}


class HashMapState[V] private(levels: IndexedSeq[Map[CandidateSet, V]])
  extends AbstractMap[CandidateSet, V]
    with StrictOptimizedIterableOps[(CandidateSet, V), Iterable, HashMapState[V]] {

  private def factory: HashMapState.type = HashMapState

  override def fromSpecific(coll: IterableOnce[(CandidateSet, V)]): HashMapState[V] = factory.fromSpecific(coll)

  override def newSpecificBuilder: mutable.Builder[(CandidateSet, V), HashMapState[V]] = factory.newSpecificBuilder

  override def empty: HashMapState[V] = factory.empty

//  @inline override def -(key: CandidateSet): HashMapState[V] = removed(key)

  override def removed(key: CandidateSet): HashMapState[V] = {
    val selectedMap = levels(key.size)
    val updatedMap = selectedMap.removed(key)
    new HashMapState(levels.updated(key.size, updatedMap))
  }

  @inline override def +[V1 >: V](kv: (CandidateSet, V1)): HashMapState[V1] = updated(kv._1, kv._2)

  override def updated[V1 >: V](key: CandidateSet, value: V1): HashMapState[V1] = {
    val b = new HashMapState.StateBuilder[V1](levels)
    b.addOne(key -> value)
    b.result()
  }

  override def get(key: CandidateSet): Option[V] =
    if (key.size >= levels.size)
      None
    else
      levels.apply(key.size).get(key)

  override def iterator: Iterator[(CandidateSet, V)] = levels.iterator.flatMap(_.iterator)

//  override def updatedWith[V1 >: V](key: CandidateSet)(remappingFunction: Option[V] => Option[V1]): HashMapState[V1] = {
//    val previousValue = this.get(key)
//    val nextValue = remappingFunction(previousValue)
//    (previousValue, nextValue) match {
//      case (None, None) => this.asInstanceOf[HashMapState[V1]]
//      case (Some(_), None) => this.removed(key).asInstanceOf[HashMapState[V1]]
//      case (_, Some(v)) => this.updated(key, v)
//    }
//  }

  // we only call updatedWith with the same value type
  def updatedWith(key: CandidateSet)(remappingFunction: Option[V] => Option[V]): HashMapState[V] = {
    val previousValue = this.get(key)
    val nextValue = remappingFunction(previousValue)
    (previousValue, nextValue) match {
      case (None, None) => this
      case (Some(_), None) => this.removed(key)
      case (_, Some(v)) => this.updated(key, v)
    }
  }

  // overrides that make return types more specific
  override def concat[V1 >: V](suffix: collection.IterableOnce[(CandidateSet, V1)]): HashMapState[V1] = {
    val b = new HashMapState.StateBuilder[V1](levels)
    b.addAll(suffix)
    b.result()
  }
}