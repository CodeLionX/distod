package com.github.codelionx.util.largeMap.mutable

import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.Math
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import scala.collection.{mutable, StrictOptimizedIterableOps}
import scala.jdk.CollectionConverters._


object FastutilState {

  val DEFAULT_NUMBER_OF_ATTRIBUTES: Int = 5

  def empty[V]: FastutilState[V] = empty[V](DEFAULT_NUMBER_OF_ATTRIBUTES)

  def empty[V](nAttributes: Int): FastutilState[V] = new FastutilState(nAttributes, IndexedSeq.empty)

  def apply[V](nAttributes: Int, elems: (CandidateSet, V)*): FastutilState[V] = fromSpecific(nAttributes, elems)

  def fromSpecific[V](nAttributes: Int, coll: IterableOnce[(CandidateSet, V)]): FastutilState[V] = {
    val s = empty[V](nAttributes)
    s.addAll(coll)
    s
  }

  def newBuilder[V](nAttributes: Int): mutable.Builder[(CandidateSet, V), FastutilState[V]] =
    new mutable.GrowableBuilder[(CandidateSet, V), FastutilState[V]](empty[V](nAttributes))
}


class FastutilState[V] private(
    private var nAttributes: Int,
    private var levels: IndexedSeq[java.util.Map[CandidateSet, V]]
)
  extends mutable.AbstractMap[CandidateSet, V]
    with StrictOptimizedIterableOps[(CandidateSet, V), mutable.Iterable, FastutilState[V]] {

  private def factory: FastutilState.type = FastutilState

  override def fromSpecific(coll: IterableOnce[(CandidateSet, V)]): FastutilState[V] =
    factory.fromSpecific(nAttributes, coll)

  override def newSpecificBuilder: mutable.Builder[(CandidateSet, V), FastutilState[V]] =
    factory.newBuilder(nAttributes)

  override def empty: FastutilState[V] = factory.empty(nAttributes)

  override def apply(key: CandidateSet): V =
    if (key.size >= levels.size)
      throw new NoSuchElementException(s"Map for size ${key.size} not initialized")
    else
      levels(key.size).get(key) match {
        case null => throw new NoSuchElementException("Key not found in map")
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

  override def subtractOne(key: CandidateSet): FastutilState.this.type = {
    if (key.size < levels.size) {
      val selectedMap = levels(key.size)
      selectedMap.remove(key)
    }
    this
  }

  override def addOne(elem: (CandidateSet, V)): FastutilState.this.type = {
    val (key, value) = elem
    while (levels.size <= key.size) {
      levels :+= new java.util.concurrent.ConcurrentHashMap()
//      levels :+= new Object2ObjectOpenHashMap()
    }
    levels(key.size).put(key, value)
    this
  }

  override def iterator: Iterator[(CandidateSet, V)] = levels.iterator
    .flatMap(
      _.entrySet()
        .asScala
        .map(entry => entry.getKey -> entry.getValue)
    )
}
