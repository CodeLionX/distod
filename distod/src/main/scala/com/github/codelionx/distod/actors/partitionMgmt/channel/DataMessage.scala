package com.github.codelionx.distod.actors.partitionMgmt.channel

import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet


trait DataMessage extends CborSerializable

// should be top-level for serialization
final case class AttributeData(attributes: Seq[Int]) extends DataMessage
final case class EmptyPartitionData(value: StrippedPartition) extends DataMessage
final case class FullPartitionData(key: CandidateSet, value: FullPartition) extends DataMessage

object DataMessage extends DataMessageAkkaSerialization
