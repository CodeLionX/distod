package com.github.codelionx.distod.partitions

import scala.collection.{SortedMap, mutable}


/**
 * Base trait for partitions. The equivalence classes of a partition are stored in a SortedMap.
 *
 * @see [[com.github.codelionx.distod.partitions.FullPartition]]
 * @see [[com.github.codelionx.distod.partitions.StrippedPartition]]
 */
trait Partition {

  def numberElements: Int

  def numberClasses: Int

  def equivClasses: SortedMap[Value, Set[Index]]

  /**
   * Converts this partition to a stripped partition by removing equivalence classes with size 1.
   */
  def stripped: StrippedPartition = Partition.toStripped(this)
}


object Partition {

  /**
   * Partitions the column's values into equivalence classes stored as a partition.
   *
   * This first infers the type of the column, then uses the type's ordering to create the equivalence classes, and
   * finally stores the equivalence classes in a [[scala.collection.SortedMap]].
   */
  def fullFrom(column: Array[String]): FullPartition = {
    val equivalenceClasses = partitionColumn(column)
    FullPartition(
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
   * the equivalence classes in a [[scala.collection.SortedMap]], and finally removes all equivalence classes of size 1.
   */
  def strippedFrom(column: Array[String]): StrippedPartition =
    fullFrom(column).stripped

  /**
   * Removes equivalence classes of size 1 from a partition. `StrippedPartition`s are returned as is.
   */
  def toStripped(partition: Partition): StrippedPartition = partition match {
    case p: FullPartition =>
      val strippedClasses = stripClasses(p.equivClasses)
      StrippedPartition(
        numberElements = strippedClasses.valuesIterator.map(_.size).sum,
        numberClasses = strippedClasses.size,
        equivClasses = strippedClasses
      )
    case p: StrippedPartition => p
  }

  private def partitionColumn(column: Array[String]): SortedMap[Value, Set[Index]] = {
    val tpe = TypeInferrer.inferTypeForColumn(column)
    val valueMap: mutable.Map[String, mutable.Set[Index]] = mutable.HashMap.empty

    for ((entry, i) <- column.zipWithIndex) {
      val equivClass = valueMap.getOrElseUpdate(entry, mutable.HashSet.empty)
      equivClass.add(i)
    }

    val sortedIndexedKeys = valueMap.keys.toSeq.sortWith(tpe.valueLt).zipWithIndex

    val classes: mutable.SortedMap[Value, Set[Index]] = mutable.SortedMap.empty
    for ((key, id) <- sortedIndexedKeys) {
      classes.addOne(id -> valueMap(key).toSet)
    }
    classes
  }

  private def stripClasses(classes: SortedMap[Value, Set[Index]]): SortedMap[Value, Set[Index]] =
    classes.filterNot { case (_, indexSet) => indexSet.size <= 1 }
}