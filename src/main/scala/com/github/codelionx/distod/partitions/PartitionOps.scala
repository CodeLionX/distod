package com.github.codelionx.distod.partitions

import scala.collection.mutable


trait PartitionOps { this: Partition =>

  /**
   * Converts this partition to a stripped partition by removing equivalence classes with size 1.
   */
  @inline def stripped: StrippedPartition = toStripped

  /**
   * Alias to [[com.github.codelionx.distod.partitions.PartitionOps#product]].
   */
  @inline def *(other: Partition): Partition = product(other)

  /**
   * Computes the product of two partitions. The product of two partitions is the least refined partition that refines
   * both this and `other`.
   *
   * This operation is only defined for two partitions of the same type. Either both are
   * [[com.github.codelionx.distod.partitions.FullPartition]]s or
   * [[com.github.codelionx.distod.partitions.StrippedPartition]]s.
   *
   * @param other the other partition
   * @throws IllegalArgumentException if two different partition types should be multiplied
   */
  def product(other: Partition): Partition = product(this, other)

  private def toStripped: StrippedPartition = this match {
    case p: FullPartition =>
      val strippedClasses = stripClasses(p.equivClasses)
      StrippedPartition(
        nTuples = p.nTuples,
        numberElements = strippedClasses.map(_.size).sum,
        numberClasses = strippedClasses.size,
        equivClasses = strippedClasses
      )
    case p: StrippedPartition => p
  }

  private def product(partition1: Partition, partition2: Partition): Partition = (partition1, partition2) match {
    // if both partitions are the same, we do not need to perform any computation
    case (p1, p2) if p1 equals p2 => p1

    case (_: FullPartition, _: StrippedPartition) | (_: StrippedPartition, _: FullPartition) =>
      throw new IllegalArgumentException("Can not build product of full and stripped partitions")

    case (p1: FullPartition, p2: FullPartition) =>
      // only intersect sets with > 1 elements
      val (p1Classes, singletonP1Classes) = p1.equivClasses.partition(_.size > 1)
      val (p2Classes, singletonP2Classes) = p2.equivClasses.partition(_.size > 1)
      val newClasses: Iterable[Set[Index]] = for {
        x <- p1Classes
        y <- p2Classes
        newClass = x.intersect(y)
        if newClass.nonEmpty
      } yield newClass

      val equivClasses = (singletonP1Classes ++ singletonP2Classes).distinct ++ newClasses
      FullPartition(
        nTuples = scala.math.max(p1.nTuples, p2.nTuples),
        numberElements = p1.numberElements, // we include all elements, so save summing it up
        numberClasses = equivClasses.size,
        equivClasses = equivClasses
      )

    case (p1: StrippedPartition, p2: StrippedPartition) =>
      fastProduct(p1, p2)
  }

  private def setIntersectionProduct(
      partition1: StrippedPartition, partition2: StrippedPartition
  ): StrippedPartition = {
    val newClasses: IndexedSeq[Set[Index]] = for {
      x <- partition1.equivClasses
      y <- partition2.equivClasses
      newClass = x.intersect(y)
      if newClass.size > 1
    } yield newClass

    StrippedPartition(
      nTuples = scala.math.max(partition1.nTuples, partition2.nTuples),
      numberElements = newClasses.map(_.size).sum,
      numberClasses = newClasses.size,
      equivClasses = newClasses
    )
  }

  private def fastProduct(partition1: StrippedPartition, partition2: StrippedPartition): StrippedPartition = {
    val nTuples = scala.math.max(partition1.nTuples, partition2.nTuples)
    val tempClasses = Array.fill(partition1.numberClasses)(Set.newBuilder[Int])
    val resultClasses = mutable.Buffer.empty[Set[Int]]
    val lut = Array.fill(nTuples)(-1)

    // fill lut from first partition
    val classes1 = partition1.equivClasses
    for ((tupleIdSet, classIndex) <- classes1.zipWithIndex) {
      for (tupleId <- tupleIdSet) {
        lut(tupleId) = classIndex
      }
    }
    // iterate over second partition and compare to lut
    val classes2 = partition2.equivClasses
    for (tupleIdSet <- classes2) {
      val remainingIds = tupleIdSet.filter(id => lut(id) != -1)
      remainingIds.foreach { id =>
        tempClasses(lut(id)).addOne(id)
      }
      // for each set emit a new result set if it contains more than 2 elems
      for (tupleId <- remainingIds) {
        val classBuilder = tempClasses(lut(tupleId))
        val classResult = classBuilder.result()
        if (classResult.knownSize > 1) {
          resultClasses.append(classResult)
        }
        classBuilder.clear()
      }
    }
    val newClasses = resultClasses.toIndexedSeq
    StrippedPartition(
      nTuples = nTuples,
      numberElements = newClasses.map(_.size).sum,
      numberClasses = newClasses.size,
      equivClasses = newClasses
    )
  }

  private def stripClasses(classes: IndexedSeq[Set[Index]]): IndexedSeq[Set[Index]] =
    classes.filterNot { indexSet => indexSet.size <= 1 }
}
