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

  private def expectedSize(n: Long, k: Long): Int = {
    val maxValue = Int.MaxValue / 2
    val maxLevelSize = Math.binomialCoefficient(n, k)
    scala.math.min(maxLevelSize, maxValue).toInt
  }
}


class FastutilState[V] private(
    private var nAttributes: Int,
    private var levels: IndexedSeq[Object2ObjectOpenHashMap[CandidateSet, V]]
)
  extends mutable.AbstractMap[CandidateSet, V]
    with StrictOptimizedIterableOps[(CandidateSet, V), mutable.Iterable, FastutilState[V]] {

  /**
   * Adjusts the array sizes of the internal maps to offer the best performance considering the expected number of
   * elements per level. The expected number of elements is calculated from the level index and the supplied `size`.
   * This method rebuilds all internal data structures (full copy) and therefore has a considerably performance hit.
   * Use it sparsely.
   *
   * @param size number of attributes which form the candidate space
   */
  def reshapeMaps(size: Int): FastutilState.this.type = {
    nAttributes = size
    val reshapedMaps = levels.map { hashMap =>
      val updatedMap = new Object2ObjectOpenHashMap[CandidateSet, V]()
      updatedMap.putAll(hashMap)
      updatedMap
    }
    levels = reshapedMaps
    this
  }

  def sizeLevels: Int = levels.size

  def clearLevel(l: Int): FastutilState.this.type = {
    if (l < levels.size) {
      levels = levels.updated(l, new Object2ObjectOpenHashMap(0, 1))
    }
    this
  }

  def forallInLevel(l: Int, p: (CandidateSet, V) => Boolean): Boolean = {
    if (l >= levels.size) {
      false
    } else {
      val currentLevel = levels(l)
      var res = true
      val it = currentLevel.object2ObjectEntrySet().fastIterator()
      while (res && it.hasNext) {
        val entry = it.next()
        res = p(entry.getKey, entry.getValue)
      }
      res
    }
  }

  def filterInLevel(l: Int, pred: ((CandidateSet, V)) => Boolean): Seq[(CandidateSet, V)] = if (l >= levels.size) {
    Seq.empty
  } else {
    val currentLevel = levels(l)
    currentLevel.object2ObjectEntrySet().fastIterator().asScala.map(entry => entry.getKey -> entry.getValue).filter(pred).toSeq
  }

  def status: String = s"$size entries. By-level" +
    levels.zipWithIndex.map { case (v, i) =>
      val levelSize = v.size()
      val maxLevelSize = Math.binomialCoefficient(nAttributes, i)
      s"level $i: $levelSize elems (max=$maxLevelSize)"
    }.mkString("[\n  ", "\n  ", "\n]")

  private def factory: FastutilState.type = FastutilState

  override def fromSpecific(coll: IterableOnce[(CandidateSet, V)]): FastutilState[V] =
    factory.fromSpecific(nAttributes, coll)

  override def newSpecificBuilder: mutable.Builder[(CandidateSet, V), FastutilState[V]] =
    factory.newBuilder(nAttributes)

  override def empty: FastutilState[V] = factory.empty(nAttributes)

  override def size: Int = levels.map(_.size()).sum

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
      levels :+= new Object2ObjectOpenHashMap()
    }
    levels(key.size).put(key, value)
    this
  }

  def addAll(xs: Seq[(CandidateSet, V)]): FastutilState.this.type = addAll(xs.toMap)

  def addAll(xs: Map[CandidateSet, V]): FastutilState.this.type = if (xs.nonEmpty) {
    val maxKeySize = xs.map(_._1.size).max
    while (levels.size <= maxKeySize) {
      val expectedSize = FastutilState.expectedSize(nAttributes, levels.size)
      levels :+= new Object2ObjectOpenHashMap(expectedSize, 1)
    }
    for ((l, elems) <- xs.groupBy(_._1.size)) {
      levels(l).putAll(elems.asJava)
    }
    this
  } else {
    this
  }

  override def iterator: Iterator[(CandidateSet, V)] = levels.iterator
    .flatMap(
      _.object2ObjectEntrySet().fastIterator()
        .asScala
        .map(entry => entry.getKey -> entry.getValue)
    )

  override def keysIterator: Iterator[CandidateSet] = levels.iterator
    .flatMap(
      _.keySet().asScala
    )
}
