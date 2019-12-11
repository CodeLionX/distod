package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.partitions.StrippedPartition
import com.github.codelionx.distod.types.CandidateSet


case class ComputePartitionProductJob(
    key: CandidateSet,
    partitionA: Either[CandidateSet, StrippedPartition],
    partitionB: Either[CandidateSet, StrippedPartition]
)