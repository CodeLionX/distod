package com.github.codelionx.distod.partitions

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.types.{EquivClass, TupleValueMap}

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

  @JsonDeserialize(contentAs = classOf[EquivClass.IMPL_TYPE])
  def equivClasses: IndexedSeq[EquivClass.TYPE]

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
    @JsonDeserialize(contentAs = classOf[EquivClass.IMPL_TYPE])
    equivClasses: IndexedSeq[EquivClass.TYPE],
    @JsonDeserialize(as = classOf[TupleValueMap.IMPL_TYPE])
    tupleValueMap: TupleValueMap.TYPE
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
    equivClasses: IndexedSeq[EquivClass.TYPE]
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

  private def partitionColumn(column: Array[String]): IndexedSeq[EquivClass.TYPE] = {
    val tpe = TypeInferrer.inferTypeForColumn(column)
    val valueMap: mutable.Map[String, EquivClass.IMPL_TYPE] = mutable.HashMap.empty

    for ((entry, i) <- column.zipWithIndex) {
      val equivClass = valueMap.getOrElseUpdate(entry, EquivClass(column.length, 1f))
      equivClass.add(i)
    }

    val sortedKeys =
      valueMap
        .keys
        .toArray
        .sortWith(tpe.valueLt)
    sortedKeys.map { key =>
      val set = valueMap(key)
      set.trim()
      set
    }
  }


  /**
   * Removes singleton equivalence classes
   */
  private[partitions] def stripClasses(classes: IndexedSeq[EquivClass.TYPE]): IndexedSeq[EquivClass.TYPE] =
    classes.filterNot { indexSet => indexSet.size <= 1 }

  /**
   * Converts the equivalence classes of a full partition to a tuple-value-map, mapping each tuple ID to its value
   * equivalent (position of the corresponding equivalence class in the sorted partition).
   *
   * @return Map containing tuple ID to value mapping
   */
  private[partitions] def convertToTupleValueMap(equivClasses: IndexedSeq[EquivClass.TYPE]): TupleValueMap.TYPE =
    fastTupleValueMapper(equivClasses)

//  private def functionalTupleValueMapper(equivClasses: IndexedSeq[IntSet]): Int2IntMap = {
//    val indexedClasses = equivClasses.zipWithIndex
//    indexedClasses.flatMap {
//      case (set, value) => set.map(_ -> value)
//    }.toMap
//  }

  private def fastTupleValueMapper(equivClasses: IndexedSeq[EquivClass.TYPE]): TupleValueMap.TYPE = {
    val size = equivClasses.map(_.size()).sum
    val map = TupleValueMap(size, 1f)

    for( (set, value) <- equivClasses.zipWithIndex) {
      val iter = set.iterator
      while(iter.hasNext) {
        map.put(iter.nextInt, value)
      }
    }
    map.trim()
    map
  }
}