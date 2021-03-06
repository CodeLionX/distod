package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.stream.typed.scaladsl.ActorSink
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.Master
import com.github.codelionx.distod.actors.master.MasterHelper.GetPrimaryPartitionManager
import com.github.codelionx.distod.actors.partitionMgmt.channel._
import com.github.codelionx.distod.actors.partitionMgmt.channel.PartitionManagerEndpoint.{ConnectionOpened, SendAttributes, SendEmptyPartition, SendFullPartition, TeardownChannel}
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
        behavior(context, local, controller, 1)
    }
  }

  private def behavior(
      context: ActorContext[Command],
      local: ActorRef[PartitionCommand],
      primary: ActorRef[PartitionManagerEndpoint.Command],
      remainingDataMessages: Int
  ): Behavior[Command] = {
    def next(newRemaining: Int = remainingDataMessages - 1): Behavior[Command] = {
      if(newRemaining == 0) {
        primary ! TeardownChannel
        finished(context)
      } else {
        behavior(context, local, primary, newRemaining)
      }
    }

    Behaviors.receiveMessage{
      // stream based:
      case StreamInit(ackTo) =>
        ackTo ! StreamAck
        context.log.info("Stream initialized")
        Behaviors.same

      case WrappedDataMessage(ackTo, AttributeData(attributes)) =>
        ackTo ! StreamAck
        context.log.debug("{} attributes received, loading initial partition set...", attributes.size)
        local ! SetAttributes(attributes)

        primary ! SendEmptyPartition
        attributes.foreach(a =>
          primary ! SendFullPartition(CandidateSet.from(a))
        )
        // received attributes (-1) and new empty partition (+1) cancel each other out:
        next(remainingDataMessages + attributes.size)

      case WrappedDataMessage(ackTo, FullPartitionData(id, partition)) =>
        ackTo ! StreamAck
        context.log.debug("Received full partition for key {}: {}", id, partition.equivClasses.length)
        local ! InsertPartition(id, partition)
        next()

      case WrappedDataMessage(ackTo, EmptyPartitionData(value)) =>
        ackTo ! StreamAck
        context.log.debug("Received empty partition: {}", value.equivClasses.length)
        local ! InsertPartition(CandidateSet.empty, value)
        next()

      case StreamComplete =>
        context.log.error("Stream finished too early, still missing partitions.")
        Behaviors.stopped

      case StreamFailure(cause) =>
        context.log.error("Stream finished too early, still missing partitions.", cause)
        Behaviors.stopped

      // other:
      case WrappedListing(Master.MasterServiceKey.Listing(_)) =>
        context.log.error("Master service listing changed despite that we are already running!")
        Behaviors.same

      case PrimaryPartitionManager(_) | WrappedEndpointEvent(ConnectionOpened(_, _)) =>
        // ignore
        Behaviors.same
    }
  }

  private def finished(context: ActorContext[Command]): Behavior[Command] = Behaviors.receiveMessage{
    case StreamComplete =>
      context.log.info("Stream finished")
      Behaviors.stopped

    case StreamFailure(cause) =>
      context.log.error("Stream failed", cause)
      Behaviors.stopped

    case m =>
      context.log.warn("Ignoring message during channel teardown: {}", m)
      // ignore
      Behaviors.same
  }
}
