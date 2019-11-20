package com.github.codelionx.distod.protocols

import akka.actor.typed.ActorRef
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.partitions.{FullPartition, Partition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet


object PartitionManagementProtocol {

  trait PartitionCommand extends CborSerializable
  final case class LookupPartition(key: CandidateSet, replyTo: ActorRef[PartitionFound]) extends PartitionCommand
  final case class LookupStrippedPartition(key: CandidateSet, replyTo: ActorRef[StrippedPartitionFound])
    extends PartitionCommand
  final case class InsertPartition(key: CandidateSet, value: Partition) extends PartitionCommand

  trait PartitionEvent extends CborSerializable
  final case class PartitionFound(key: CandidateSet, value: FullPartition) extends PartitionEvent
  final case class StrippedPartitionFound(key: CandidateSet, value: StrippedPartition) extends PartitionEvent

//  sealed trait PartitionResultEvent extends PartitionEvent
//  case object Success extends PartitionResultEvent
//  final case class Failure(f: Throwable) extends PartitionResultEvent
}
