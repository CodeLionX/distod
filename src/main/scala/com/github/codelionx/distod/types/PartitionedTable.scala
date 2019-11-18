package com.github.codelionx.distod.types

import com.github.codelionx.distod.partitions.FullPartition


case class PartitionedTable(name: String, headers: Array[String], partitions: Array[FullPartition]) {

  /**
   * Number of rows / tuples.
   */
  lazy val n: Int = partitions.headOption match {
    case Some(c) => c.numberElements
    case None => 0
  }

  /**
   * Number of columns / attributes
   */
  lazy val m: Int = headers.length
}
