package com.github.codelionx.distod.partitions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


trait PartitionOps { this: Partition =>

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
  def product(other: Partition): Partition = (this, other) match {
    // if both partitions are the same, we do not need to perform any computation
    case (p1, p2) if p1 equals p2 => p1
    case (p1, p2) => fastProduct(p1, p2)
  }

//  private def setIntersectionProduct(partition1: Partition, partition2: Partition): Partition = {
//    val newClasses: IndexedSeq[Set[Index]] = for {
//      x <- partition1.equivClasses
//      y <- partition2.equivClasses
//      newClass = x.intersect(y)
//      if newClass.size > 1
//    } yield newClass
//
//    StrippedPartition(
//      nTuples = scala.math.max(partition1.nTuples, partition2.nTuples),
//      numberElements = newClasses.map(_.size).sum,
//      numberClasses = newClasses.size,
//      equivClasses = newClasses
//    )
//  }

  private def fastProduct(partition1: Partition, partition2: Partition): Partition = {
    val nTuples = scala.math.max(partition1.nTuples, partition2.nTuples)
    val tempClasses = Array.fill(partition1.numberClasses)(ArrayBuffer.empty[Int])
    val resultClasses = mutable.Buffer.empty[Array[Int]]
    val lut = Array.fill(nTuples)(-1)

    // fill lut from first partition
    for ((tupleIdSet, classIndex) <- partition1.equivClasses.zipWithIndex) {
      for(tupleId <- tupleIdSet)  {
        lut(tupleId) = classIndex
      }
    }
    // iterate over second partition and compare to lut
    val classes2 = partition2.equivClasses
    for (tupleIdSet <- classes2) {
      // only consider matched tupleIds and build equiv classes
      val tempClassIndices = for {
        id <- tupleIdSet
        if lut(id) != -1
        equivClassName = lut(id)
      } yield {
        tempClasses(equivClassName).addOne(id)
        equivClassName
      }
      // for each set emit a new result set if it contains more than 2 elems and clear temp data
      // no distinct here, because we reset the class and check for size > 1 anyway
      for(classIndex <- tempClassIndices) {
        val clazz = tempClasses(classIndex)
        if(clazz.size > 1) {
          val newClass = Array.ofDim[Int](clazz.size)
          clazz.copyToArray(newClass)
          resultClasses.append(newClass)
        }
        clazz.clear()
      }
    }
    val newClasses = resultClasses.toArray
    StrippedPartition(
      nTuples = nTuples,
      numberElements = newClasses.map(_.length).sum,
      numberClasses = newClasses.length,
      equivClasses = newClasses
    )
  }

}
