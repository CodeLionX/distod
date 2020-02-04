package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.stream.typed.scaladsl.ActorSink
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.Master
import com.github.codelionx.distod.actors.master.MasterHelper.GetPrimaryPartitionManager
import com.github.codelionx.distod.actors.partitionMgmt.channel._
import com.github.codelionx.distod.actors.partitionMgmt.channel.PartitionManagerEndpoint.{ConnectionOpened, SendAttributes, SendEmptyPartition, SendFullPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.CandidateSet


object PartitionReplicator {

  sealed trait Command
  final case class PrimaryPartitionManager(ref: ActorRef[PartitionCommand]) extends Command with CborSerializable
  private final case class WrappedListing(listing: Receptionist.Listing) extends Command
  private final case class WrappedEndpointEvent(message: PartitionManagerEndpoint.Event) extends Command
  private final case class WrappedDataMessage(
      ackTo: ActorRef[StreamAck.type], message: DataMessage
  ) extends Command

  private case class StreamInit(ackTo: ActorRef[StreamAck.type]) extends Command
  private case object StreamComplete extends Command
  private final case class StreamFailure(cause: Throwable) extends Command

  private case object StreamAck

  val name = "partition-replicator"

  def apply(local: ActorRef[PartitionCommand]): Behavior[Command] = Behaviors.setup { context =>
    val listingAdapter = context.messageAdapter(WrappedListing)
    val endpointAdapter = context.messageAdapter(WrappedEndpointEvent)
    val selfSink = ActorSink.actorRefWithBackpressure(
      ref = context.self,
      messageAdapter = WrappedDataMessage,
      onInitMessage = StreamInit,
      ackMessage = StreamAck,
      onCompleteMessage = StreamComplete,
      onFailureMessage = StreamFailure
    )
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
        context.log.trace("Primary found, connecting ...")
        ref ! OpenConnection(endpointAdapter)
        Behaviors.same

      case WrappedEndpointEvent(ConnectionOpened(controller, channel)) =>
        context.log.info("Connection to primary established")
        context.log.debug("Loading attributes ...")
        controller ! SendAttributes

        StreamChannel.consumeSourceRefWith(channel, selfSink, context.system)
        behavior(context, local, controller)
    }
  }

  private def behavior(
      context: ActorContext[Command],
      local: ActorRef[PartitionCommand],
      primary: ActorRef[PartitionManagerEndpoint.Command]
  ): Behavior[Command] = Behaviors.receiveMessage {

    // stream based:
    case StreamInit(ackTo) =>
      ackTo ! StreamAck
      context.log.info("Stream initialized")
      Behaviors.same

    case WrappedDataMessage(ackTo, AttributeData(attributes)) =>
      ackTo ! StreamAck
      context.log.info("{} attributes received, loading initial partition set...", attributes.size)
      local ! SetAttributes(attributes)
      primary ! SendEmptyPartition
      attributes.foreach(a =>
        primary ! SendFullPartition(CandidateSet.from(a))
      )
      Behaviors.same

    case WrappedDataMessage(ackTo, FullPartitionData(id, partition)) =>
      ackTo ! StreamAck
      context.log.debug("Received full partition for key {}: {}", id, partition.equivClasses.size)
      local ! InsertPartition(id, partition)
      Behaviors.same

    case WrappedDataMessage(ackTo, EmptyPartitionData(value)) =>
      ackTo ! StreamAck
      context.log.debug("Received empty partition: {}", value.equivClasses.size)
      local ! InsertPartition(CandidateSet.empty, value)
      Behaviors.same

    case StreamComplete =>
      context.log.info("Stream finished")
      Behaviors.stopped

    case StreamFailure(cause) =>
      context.log.error("Stream failed", cause)
      Behaviors.stopped

    // other:
    case WrappedListing(Master.MasterServiceKey.Listing(_)) =>
      context.log.error("Master service listing changed despite that we are already running!")
      Behaviors.same

    case PrimaryPartitionManager(_) | WrappedEndpointEvent(ConnectionOpened(_, _)) =>
      // ignore
      Behaviors.same

    case m =>
      context.log.warn("received unknown msg: {}", m)
      Behaviors.same
  }
}
