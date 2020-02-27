package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet


object ComputePartitionProductJob {
  sealed trait ProductJobType
  final case class StrippedPartitionType(p: StrippedPartition) extends ProductJobType
  final case class PreviouslyComputedType(key: CandidateSet) extends ProductJobType
//  final case class ComputeFromSingletonsType(key: CandidateSet, partitions: Seq[FullPartition]) extends ProductJobType
}


sealed trait ComputePartitionProductJob {

  def key: CandidateSet

  def store: Boolean
}


case class ComputeFromPredecessorsProduct(
    key: CandidateSet,
    partitionA: ComputePartitionProductJob.ProductJobType,
    partitionB: ComputePartitionProductJob.ProductJobType,
    store: Boolean
) extends ComputePartitionProductJob

case class ComputeFromSingletonsProduct(
    key: CandidateSet,
    partitions: Seq[FullPartition],
    store: Boolean
) extends ComputePartitionProductJob
