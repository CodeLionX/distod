package com.github.codelionx.util.largeMap

import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.Math
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import scala.collection.{mutable, StrictOptimizedIterableOps}
import scala.collection.immutable.{AbstractMap, Iterable}
import scala.jdk.CollectionConverters._


object FastutilState {

  def empty[V](nAttributes: Int): FastutilState[V] = new FastutilState(nAttributes, IndexedSeq.empty)

  def apply[V](nAttributes: Int, elems: (CandidateSet, V)*): FastutilState[V] = fromSpecific(nAttributes, elems)

  def fromSpecific[V](nAttributes: Int, coll: IterableOnce[(CandidateSet, V)]): FastutilState[V] = {
    val b = newSpecificBuilder[V](nAttributes)
    b.addAll(coll)
    b.result()
  }

  def newSpecificBuilder[V](nAttributes: Int): mutable.Builder[(CandidateSet, V), FastutilState[V]] =
    new StateBuilder[V](nAttributes)

  private[FastutilState] class StateBuilder[V] private[FastutilState](
      numberOfAttributes: Int,
      startState: IndexedSeq[Object2ObjectOpenHashMap[CandidateSet, V]] = IndexedSeq.empty
  ) extends mutable.Builder[(CandidateSet, V), FastutilState[V]] {

    private var internalState: IndexedSeq[Object2ObjectOpenHashMap[CandidateSet, V]] = startState

    override def addOne(elem: (CandidateSet, V)): this.type = {
      val (key, value) = elem
      // fill seq up with empty maps (prevents IndexOutOfBoundsException)
      while (internalState.size <= key.size) {
        val expectedSize = Math.binomialCoefficient(numberOfAttributes, internalState.size)
        internalState :+= new Object2ObjectOpenHashMap(expectedSize, .9f)
      }
      // select correct map and update it with new mapping
      val map = internalState(key.size)
      map.put(key, value)
      this
    }

    override def clear(): Unit = {
      internalState = IndexedSeq.empty
    }

    override def result(): FastutilState[V] = new FastutilState(
      numberOfAttributes,
      internalState
    )
  }
}


class FastutilState[V] private(nAttributes: Int, levels: IndexedSeq[Object2ObjectOpenHashMap[CandidateSet, V]])
  extends AbstractMap[CandidateSet, V]
    with StrictOptimizedIterableOps[(CandidateSet, V), Iterable, FastutilState[V]] {

  private def factory: FastutilState.type = FastutilState

  override def fromSpecific(coll: IterableOnce[(CandidateSet, V)]): FastutilState[V] =
    factory.fromSpecific(nAttributes, coll)

  override def newSpecificBuilder: mutable.Builder[(CandidateSet, V), FastutilState[V]] =
    factory.newSpecificBuilder(nAttributes)

  override def empty: FastutilState[V] = factory.empty(nAttributes)

//  @inline override def -(key: CandidateSet): FastutilState[V] = removed(key)

  override def removed(key: CandidateSet): FastutilState[V] = {
    val selectedMap = levels(key.size)
//    val updatedMap = selectedMap.clone()
    selectedMap.remove(key)
//    new FastutilState(nAttributes, levels.updated(key.size, updatedMap))
    this
  }

  @inline override def +[V1 >: V](kv: (CandidateSet, V1)): FastutilState[V1] = updated(kv._1, kv._2)

  override def updated[V1 >: V](key: CandidateSet, value: V1): FastutilState[V1] = {
//    val levelIndex = key.size
//    val levelClone = levels.zipWithIndex.map{
//      case (m, `levelIndex`) => new Object2ObjectOpenHashMap[CandidateSet, V1](m, .9f)
//      case (m, _) => m.asInstanceOf[Object2ObjectOpenHashMap[CandidateSet, V1]]
//    }
//    val b = new FastutilState.StateBuilder[V1](nAttributes, cloneLevels[V1])
//    b.addOne(key -> value)
//    b.result()
    var l = levels
    while (l.size <= key.size) {
      val expectedSize = Math.binomialCoefficient(nAttributes, l.size)
      l :+= new Object2ObjectOpenHashMap(expectedSize, .9f)
    }
    val l2 = l.asInstanceOf[IndexedSeq[Object2ObjectOpenHashMap[CandidateSet, V1]]]
    l2(key.size).put(key, value)
    new FastutilState[V1](nAttributes, l2)
  }

  override def apply(key: CandidateSet): V =
    if (key.size >= levels.size)
      throw new RuntimeException(s"Map for size ${key.size} not initialized")
    else
      levels(key.size).get(key) match {
        case null => throw new RuntimeException("Key not found in map")
        case s => s
      }

  override def get(key: CandidateSet): Option[V] =
    if (key.size >= levels.size)
      None
    else {
      levels(key.size).get(key) match {
        case null => None
        case v => Some(v)
      }
    }

  override def iterator: Iterator[(CandidateSet, V)] = levels.iterator.flatMap(_.entrySet().asScala.map(e =>
    e.getKey -> e.getValue
  ))

//  override def updatedWith[V1 >: V](key: CandidateSet)(remappingFunction: Option[V] => Option[V1]): FastutilState[V1] = {
//    val previousValue = this.get(key)
//    val nextValue = remappingFunction(previousValue)
//    (previousValue, nextValue) match {
//      case (None, None) => this.asInstanceOf[FastutilState[V1]]
//      case (Some(_), None) => this.removed(key).asInstanceOf[FastutilState[V1]]
//      case (_, Some(v)) => this.updated(key, v)
//    }
//  }

  // we only call updatedWith with the same value type
  def updatedWith(key: CandidateSet)(remappingFunction: Option[V] => Option[V]): FastutilState[V] = {
    val previousValue = this.get(key)
    val nextValue = remappingFunction(previousValue)
    (previousValue, nextValue) match {
      case (None, None) => this
      case (Some(_), None) => this.removed(key)
      case (_, Some(v)) => this.updated(key, v)
    }
  }

  // overrides that make return types more specific
  override def concat[V1 >: V](suffix: collection.IterableOnce[(CandidateSet, V1)]): FastutilState[V1] = {
    val b = new FastutilState.StateBuilder[V1](nAttributes, cloneLevels[V1])
    b.addAll(suffix)
    b.result()
  }

  private def cloneLevels[V1 >: V]: IndexedSeq[Object2ObjectOpenHashMap[CandidateSet, V1]] =
//    levels.map(orig => new Object2ObjectOpenHashMap[CandidateSet, V1](orig, .9f))
    levels.map(_.asInstanceOf[Object2ObjectOpenHashMap[CandidateSet, V1]])
}
