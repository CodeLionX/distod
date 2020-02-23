package com.github.codelionx.distod.types

import com.github.codelionx.distod.partitions.FullPartition


case class PartitionedTable(name: String, headers: Array[String], partitions: Array[FullPartition]) {

  /**
   * Number of rows / tuples.
   */
  def nTuples: Int = partitions.headOption match {
    case Some(c) => c.nTuples
    case None => 0
  }

  /**
   * Number of columns / attributes
   */
  def nAttributes: Int = headers.length
}
