package com.github.codelionx.distod.protocols

import akka.actor.typed.ActorRef
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.types.PartitionedTable


object DataLoadingProtocol {

  trait DataLoadingCommand extends CborSerializable
  final case class LoadPartitions(replyTo: ActorRef[DataLoadingEvent]) extends DataLoadingCommand
  case object Stop extends DataLoadingCommand

  sealed trait DataLoadingEvent extends CborSerializable
  final case class PartitionsLoaded(table: PartitionedTable) extends DataLoadingEvent

}
