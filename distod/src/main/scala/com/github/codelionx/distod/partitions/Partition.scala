package com.github.codelionx.distod.partitions

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.codelionx.distod.Serialization.CborSerializable

import scala.collection.immutable.HashMap
import scala.collection.mutable


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

  @JsonDeserialize(contentAs = classOf[Set[Index]])
  def equivClasses: IndexedSeq[Set[Index]]

  def error: Double = numberElements - numberClasses
}


/**
 * A partition that retains the original ordering by storing a tuple value map next to the stripped equivalence
 * classes (= classes of size one removed).
 *
 * @param nTuples        original number of tuples of the dataset
 * @param numberElements number of all elements in all remaining equivalence classes
 * @param numberClasses  number of remaining equivalence classes
 * @param equivClasses   sorted stripped equivalence classes
 * @param tupleValueMap  map from tuple ID to its value equivalent (position of the corresponding equivalence class
 *                       in the sorted partition)
 */
case class FullPartition private[partitions](
    nTuples: Int,
    numberElements: Int,
    numberClasses: Int,
    equivClasses: IndexedSeq[Set[Index]],
    tupleValueMap: Map[Index, Value]
) extends Partition {

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
    equivClasses: IndexedSeq[Set[Index]]
) extends Partition


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
    val strippedClasses = stripClasses(equivalenceClasses)
    FullPartition(
      nTuples = column.length,
      numberElements = strippedClasses.map(_.size).sum,
      numberClasses = strippedClasses.size,
      equivClasses = strippedClasses,
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


  /**
   * Removes singleton equivalence classes
   */
  private[partitions] def stripClasses(classes: IndexedSeq[Set[Index]]): IndexedSeq[Set[Index]] =
    classes.filterNot { indexSet => indexSet.size <= 1 }

  /**
   * Converts the equivalence classes of a full partition to a tuple-value-map, mapping each tuple ID to its value
   * equivalent (position of the corresponding equivalence class in the sorted partition).
   *
   * @return Map containing tuple ID to value mapping
   */
  private[partitions] def convertToTupleValueMap(equivClasses: IndexedSeq[Set[Index]]): Map[Index, Value] =
    fastTupleValueMapper(equivClasses)

  private def functionalTupleValueMapper(equivClasses: IndexedSeq[Set[Index]]): Map[Index, Value] = {
    val indexedClasses = equivClasses.zipWithIndex
    indexedClasses.flatMap {
      case (set, value) => set.map(_ -> value)
    }.toMap
  }

  private def fastTupleValueMapper(equivClasses: IndexedSeq[Set[Index]]): Map[Index, Value] = {
    val builder = HashMap.newBuilder[Index, Value]
    equivClasses.zipWithIndex.foreach { case (set, value) =>
      set.foreach(index =>
        builder.addOne(index, value)
      )
    }
    builder.result()
  }
}