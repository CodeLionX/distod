package com.github.codelionx.distod.partitions

import java.util.Objects

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.types.TupleValueMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
 * Base trait for partitions. The equivalence classes of a partition are stored in a
 * [[scala.collection.immutable.IndexedSeq]].
 * Semantically this represents a stripped partition with equivalence classes of size one already removed. You can
 * pattern match on this trait to find out if it is a [[com.github.codelionx.distod.partitions.FullPartition]] to get
 * access on the tuple value map with the original ordering.
 *
 * @see [[com.github.codelionx.distod.partitions.FullPartition]]
 * @see [[com.github.codelionx.distod.partitions.StrippedPartition]]
 */
sealed trait Partition extends CborSerializable with PartitionOps {

  def nTuples: Int

  def numberElements: Int

  def numberClasses: Int

  @JsonDeserialize(contentAs = classOf[Array[Int]])
  def equivClasses: Array[Array[Int]]

  def error: Double = numberElements - numberClasses

  override def hashCode(): Value = Objects.hash(nTuples, numberElements, numberClasses, equivClasses)

  override def equals(o: Any): Boolean = o match {
    case p: Partition =>
      this.nTuples == p.nTuples &&
      this.numberClasses == p.numberClasses &&
      this.numberElements == p.numberElements &&
      this.equivClasses.flatten.sameElements(p.equivClasses.toIndexedSeq.flatten)
    case _ => false
  }
}


/**
 * A partition that retains the original ordering by storing a tuple value map next to the stripped equivalence
 * classes (= classes of size one removed).
 *
 * @param nTuples        original number of tuples of the dataset
 * @param tupleValueMap  map from tuple ID to its value equivalent (position of the corresponding equivalence class
 *                       in the sorted partition)
 */
case class FullPartition private[partitions](
    nTuples: Int,
    @JsonDeserialize(as = classOf[TupleValueMap.IMPL_TYPE])
    tupleValueMap: TupleValueMap.TYPE
) extends Partition {

  @transient
  lazy val equivClasses: Array[Array[Int]] = Partition.stripClasses(
    Partition.convertFromTupleValueMap(tupleValueMap)
  )

  @transient
  def numberElements: Int = equivClasses.map(_.length).sum

  @transient
  def numberClasses: Int = equivClasses.length

  /**
   * Converts this `FullPartition` to a [[com.github.codelionx.distod.partitions.StrippedPartition]] by removing the
   * tuple value map. This saves some memory.
   */
  def stripped: StrippedPartition = StrippedPartition(
    nTuples = nTuples,
    numberElements = numberElements,
    numberClasses = numberClasses,
    equivClasses = equivClasses
  )

  override def hashCode(): Value = Objects.hash(nTuples, numberElements, numberClasses, equivClasses, tupleValueMap)

  override def equals(o: Any): Boolean = o match {
    case p: FullPartition =>
      super.equals(p) && this.tupleValueMap.equals(p.tupleValueMap)
    case _ => false
  }
}


/**
 * A stripped partition with equivalence classes of size one removed.
 *
 * @param nTuples        original number of tuples of the dataset
 * @param numberElements number of all elements in all remaining equivalence classes
 * @param numberClasses  number of remaining equivalence classes
 * @param equivClasses   sorted stripped equivalence classes
 */
case class StrippedPartition private[partitions](
    nTuples: Int,
    numberElements: Int,
    numberClasses: Int,
    equivClasses: Array[Array[Int]]
) extends Partition {

  /**
   * Alias to [[com.github.codelionx.distod.partitions.StrippedPartition#product]].
   */
  @inline def *(other: StrippedPartition): StrippedPartition = product(other)

  /**
   * Specialization of [[com.github.codelionx.distod.partitions.PartitionOps#product]]
   */
  @inline def product(other: StrippedPartition): StrippedPartition =
    super.product(other).asInstanceOf[StrippedPartition]
}


object Partition {

  /**
   * Partitions the column's values into equivalence classes stored as a partition.
   *
   * This first infers the type of the column, then uses the type's ordering to create the equivalence classes, and
   * finally stores the equivalence classes in a [[scala.collection.immutable.IndexedSeq]]. The original ordering of
   * the columns is retained in a tuple value map.
   * To save memory, one can call `stripped` on a [[FullPartition]] which discards the tuple value map.
   */
  def fullFrom(column: Array[String]): FullPartition = {
    val equivalenceClasses = partitionColumn(column)
    FullPartition(
      nTuples = column.length,
      tupleValueMap = convertToTupleValueMap(equivalenceClasses)
    )
  }

  /**
   * Partitions the column's values into equivalence classes stored as a partition with equivalence classes of size 1
   * removed.
   *
   * This first infers the type of the column, then uses the type's ordering to create the equivalence classes, stores
   * the equivalence classes in a [[scala.collection.immutable.IndexedSeq]], and finally removes all equivalence
   * classes of size 1.
   */
  def strippedFrom(column: Array[String]): StrippedPartition =
    fullFrom(column).stripped

  private def partitionColumn(column: Array[String]): Array[Array[Int]] = {
    val tpe = TypeInferrer.inferTypeForColumn(column)
    val valueMap: mutable.Map[String, ArrayBuffer[Int]] = mutable.HashMap.empty

    for ((entry, i) <- column.zipWithIndex) {
      val equivClass = valueMap.getOrElseUpdate(entry, ArrayBuffer.empty)
      equivClass.addOne(i)
    }

    val sortedKeys =
      valueMap
        .keys
        .toArray
        .sortWith(tpe.valueLt)
    sortedKeys.map(key => valueMap(key).toArray)
  }


  /**
   * Removes singleton equivalence classes
   */
  private[partitions] def stripClasses(classes: Array[Array[Int]]): Array[Array[Int]] =
    classes.filterNot { indexSet => indexSet.length <= 1 }

  /**
   * Converts the equivalence classes of a full partition to a tuple-value-map, mapping each tuple ID to its value
   * equivalent (position of the corresponding equivalence class in the sorted partition).
   *
   * @return Map containing tuple ID to value mapping
   */
  private[partitions] def convertToTupleValueMap(equivClasses: Array[Array[Int]]): TupleValueMap.TYPE =
    fastTupleValueMapper(equivClasses)

//  private def functionalTupleValueMapper(equivClasses: IndexedSeq[IntSet]): Int2IntMap = {
//    val indexedClasses = equivClasses.zipWithIndex
//    indexedClasses.flatMap {
//      case (set, value) => set.map(_ -> value)
//    }.toMap
//  }

  private def fastTupleValueMapper(equivClasses: Array[Array[Int]]): TupleValueMap.TYPE = {
    val size = equivClasses.map(_.length).sum
    val map = TupleValueMap(size, 1f)

    for( (set, value) <- equivClasses.zipWithIndex; tupleId <- set) {
      map.put(tupleId, value)
    }
    map.trim()
    map
  }

  /**
   * Converts the tuple value map of a full partition back to equivalence classes.
   *
   * @return list of equivalence classes
   */
  private[partitions] def convertFromTupleValueMap(map: TupleValueMap.TYPE): Array[Array[Int]] = {
    val classMap = mutable.HashMap.empty[Int, ArrayBuffer[Int]]
    val entries = map.int2IntEntrySet().iterator()

    while (entries.hasNext) {
      val entry = entries.next()
      val clazz = classMap.getOrElseUpdate(entry.getIntValue, ArrayBuffer.empty)
      clazz.addOne(entry.getIntKey)
    }
    classMap.values.map(_.sorted.toArray).toArray
  }
}
