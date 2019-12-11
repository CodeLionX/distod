package com.github.codelionx.distod.partitions

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.codelionx.distod.Serialization.CborSerializable

import scala.collection.mutable


/**
 * Base trait for partitions. The equivalence classes of a partition are stored in a SortedMap.
 *
 * @see [[com.github.codelionx.distod.partitions.FullPartition]]
 * @see [[com.github.codelionx.distod.partitions.StrippedPartition]]
 */
sealed trait Partition extends CborSerializable with PartitionOps {

  def nTuples: Int

  def numberElements: Int

  def numberClasses: Int

  @JsonDeserialize(
    keyAs = classOf[Value],
    contentAs = classOf[Set[Index]]
  )
  def equivClasses: IndexedSeq[Set[Index]]

  def error: Double
}


/**
 * A partition with all equivalence classes.
 *
 * @param numberElements number of all elements in all equivalence classes
 * @param numberClasses  number of equivalence classes
 * @param equivClasses   sorted equivalence classes
 */
case class FullPartition private[partitions](
    nTuples: Int,
    numberElements: Int,
    numberClasses: Int,
    equivClasses: IndexedSeq[Set[Index]]
) extends Partition {

  override def error: Double = 1 - numberClasses

  override def stripped: StrippedPartition = _stripped

  private lazy val _stripped: StrippedPartition = {
    val strippedClasses = stripClasses(equivClasses)
    StrippedPartition(
      nTuples = nTuples,
      numberElements = strippedClasses.map(_.size).sum,
      numberClasses = strippedClasses.size,
      equivClasses = strippedClasses
    )
  }

  /**
   * Converts this full partition to a tuple-value-map, mapping each tuple ID to its value equivalent (position of
   * the corresponding equivalence class in the sorted partition).
   *
   * @return Map containing tuple ID to value mapping
   */
  lazy val toTupleValueMap: Map[Index, Value] = convertToTupleValueMap
}


/**
 * A stripped partition with equivalence classes of size one removed.
 *
 * @param numberElements number of all elements in all remaining equivalence classes
 * @param numberClasses  number of remaining equivalence classes
 * @param equivClasses   remaining sorted equivalence classes
 */
case class StrippedPartition private[partitions](
    nTuples: Int,
    numberElements: Int,
    numberClasses: Int,
    equivClasses: IndexedSeq[Set[Index]]
) extends Partition {

  override def error: Double = numberElements - numberClasses

  override def stripped: StrippedPartition = this
}


object Partition {

  /**
   * Partitions the column's values into equivalence classes stored as a partition.
   *
   * This first infers the type of the column, then uses the type's ordering to create the equivalence classes, and
   * finally stores the equivalence classes in a [[scala.collection.immutable.IndexedSeq]].
   */
  def fullFrom(column: Array[String]): FullPartition = {
    val equivalenceClasses = partitionColumn(column)
    FullPartition(
      nTuples = column.length,
      numberElements = column.length,
      numberClasses = equivalenceClasses.size,
      equivClasses = equivalenceClasses
    )
  }

  /**
   * Partitions the column's values into equivalence classes stored as a partition with equivalence classes of size 1
   * removed.
   *
   * This first infers the type of the column, then uses the type's ordering to create the equivalence classes, stores
   * the equivalence classes in a [[scala.collection.immutable.IndexedSeq]], and finally removes all equivalence classes of size 1.
   */
  def strippedFrom(column: Array[String]): StrippedPartition =
    fullFrom(column).stripped

  private def partitionColumn(column: Array[String]): IndexedSeq[Set[Index]] = {
    val tpe = TypeInferrer.inferTypeForColumn(column)
    val valueMap: mutable.Map[String, mutable.Set[Index]] = mutable.HashMap.empty

    for ((entry, i) <- column.zipWithIndex) {
      val equivClass = valueMap.getOrElseUpdate(entry, mutable.HashSet.empty)
      equivClass.add(i)
    }

    val sortedKeys =
      valueMap
        .keys.toIndexedSeq
        .sortWith(tpe.valueLt)
    sortedKeys.map(key => valueMap(key).toSet)
  }
}