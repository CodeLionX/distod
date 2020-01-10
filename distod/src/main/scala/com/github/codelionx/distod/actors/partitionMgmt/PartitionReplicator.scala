package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.actors.master.Master
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.Master.GetPrimaryPartitionManager
import com.github.codelionx.distod.types.CandidateSet


object PartitionReplicator {

  sealed trait Command
  final case class PrimaryPartitionManager(ref: ActorRef[PartitionCommand]) extends Command with CborSerializable
  private final case class WrappedListing(listing: Receptionist.Listing) extends Command
  private final case class WrappedPartitionEvent(message: PartitionEvent) extends Command

  val name = "partition-replicator"

  def apply(local: ActorRef[PartitionCommand]): Behavior[Command] = Behaviors.setup { context =>
    val listingAdapter = context.messageAdapter(WrappedListing)
    val partitionProtocolAdapter = context.messageAdapter(WrappedPartitionEvent)
    context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, listingAdapter)

    Behaviors.receiveMessage {
      case WrappedListing(Master.MasterServiceKey.Listing(ls)) =>
        ls.headOption match {
          case None =>
          case Some(masterRef) =>
            context.log.debug("Found master at {}. Asking for primary partition manager", masterRef)
            masterRef ! GetPrimaryPartitionManager(context.self)
        }
        Behaviors.same

      case PrimaryPartitionManager(ref) =>
        context.log.info("Connection to primary established")
        context.log.debug("Loading attributes ...")
        ref ! LookupAttributes(partitionProtocolAdapter)
        behavior(context, local, ref, partitionProtocolAdapter)
    }
  }

  private def behavior(
      context: ActorContext[Command],
      local: ActorRef[PartitionCommand],
      primary: ActorRef[PartitionCommand],
      partitionProtocolAdapter: ActorRef[PartitionEvent]
  ): Behavior[Command] = Behaviors.receiveMessage {
    case WrappedPartitionEvent(AttributesFound(attributes)) =>
      context.log.debug("{} attributes received, loading initial partition set...", attributes.size)
      local ! SetAttributes(attributes)
      attributes.foreach(a =>
        primary ! LookupPartition(CandidateSet.from(a), partitionProtocolAdapter)
      )
      primary ! LookupStrippedPartition(CandidateSet.empty, partitionProtocolAdapter)
      Behaviors.same
    case WrappedPartitionEvent(PartitionFound(id, partition)) =>
      local ! InsertPartition(id, partition)
      Behaviors.same
    case WrappedPartitionEvent(StrippedPartitionFound(key, value)) =>
      local ! InsertPartition(key, value)
      Behaviors.same
    case WrappedListing(Master.MasterServiceKey.Listing(ls)) =>
      context.log.error("Master service listing changed despite that we are already running!")
      Behaviors.same
    case m =>
      context.log.warn("received unknown msg: {}", m)
      Behaviors.same
  }
}
