package com.github.codelionx.distod.partitions

import com.github.codelionx.distod.types.EquivClass
import com.github.codelionx.distod.types.EquivClass.Implicits._

import scala.collection.mutable


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
    val tempClasses = Array.fill(partition1.numberClasses)(EquivClass(nTuples, 1f))
    val resultClasses = mutable.Buffer.empty[EquivClass.TYPE]
    val lut = Array.fill(nTuples)(-1)

    // fill lut from first partition
    val classes1 = partition1.equivClasses
    for ((tupleIdSet, classIndex) <- classes1.zipWithIndex) {
      val iter = tupleIdSet.iterator
      while(iter.hasNext) {
        val tupleId = iter.nextInt
        lut(tupleId) = classIndex
      }
    }
    // iterate over second partition and compare to lut
    val classes2 = partition2.equivClasses
    for (tupleIdSet <- classes2) {
      val remainingIds = tupleIdSet.filter(id => lut(id) != -1)
      remainingIds.foreach { id =>
        tempClasses(lut(id)).add(id)
      }
      // for each set emit a new result set if it contains more than 2 elems
      for (tupleId <- remainingIds) {
        val classResult = tempClasses(lut(tupleId))
        if (classResult.size > 1) {
          resultClasses.append(classResult.clone().trimSelf())
        }
        classResult.clear()
      }
    }
    val newClasses = resultClasses.toArray
    StrippedPartition(
      nTuples = nTuples,
      numberElements = newClasses.map(_.size).sum,
      numberClasses = newClasses.length,
      equivClasses = newClasses
    )
  }

}
