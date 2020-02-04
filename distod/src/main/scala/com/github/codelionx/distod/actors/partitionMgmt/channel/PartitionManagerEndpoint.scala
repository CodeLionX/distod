package com.github.codelionx.distod.actors.partitionMgmt.channel

import java.util.UUID

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import akka.stream._
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.partitionMgmt.CompactingPartitionMap
import com.github.codelionx.distod.types.CandidateSet

import scala.util.{Failure, Success, Try}


object PartitionManagerEndpoint {

  sealed trait Command extends CborSerializable
  case object SendAttributes extends Command
  case object SendEmptyPartition extends Command
  final case class SendFullPartition(key: CandidateSet) extends Command
  private final case class WrappedQueueOfferResult(result: QueueOfferResult, dataMessage: DataMessage) extends Command

  private object WrappedQueueOfferResult {

    def fromTry(dataMessage: DataMessage)(messageTry: Try[QueueOfferResult]): WrappedQueueOfferResult = {
      messageTry match {
        case Success(value) => WrappedQueueOfferResult(value, dataMessage)
        case Failure(ex) => WrappedQueueOfferResult(QueueOfferResult.Failure(ex), dataMessage)
      }
    }
  }

  trait Event extends CborSerializable
  final case class ConnectionOpened(controller: ActorRef[Command], channel: SourceRef[ByteString]) extends Event

  def name(): String = {
    val uuid = UUID.randomUUID().toString.split("-")(0)
    s"endpoint-$uuid"
  }

  def apply(
      partitions: CompactingPartitionMap, remote: ActorRef[Event], attributes: Seq[Int]
  ): Behavior[Command] = Behaviors.setup(context =>
    Behaviors.withStash(100)(stash =>
      new PartitionManagerEndpoint(context, stash, partitions, attributes, remote).start()
    )
  )
}


class PartitionManagerEndpoint(
    context: ActorContext[PartitionManagerEndpoint.Command],
    stash: StashBuffer[PartitionManagerEndpoint.Command],
    partitions: CompactingPartitionMap,
    attributes: Seq[Int],
    remote: ActorRef[PartitionManagerEndpoint.Event]
) {

  import PartitionManagerEndpoint._


  private val channel = openChannel()

  def start(): Behavior[Command] = Behaviors.receiveMessage {
    case SendAttributes =>
      val message = AttributeData(attributes)
      val future = channel.offer(message)
      context.pipeToSelf(future)(WrappedQueueOfferResult.fromTry(message))
      waitForResult()

    case SendEmptyPartition =>
      val p = partitions(CandidateSet.empty)
      val message = EmptyPartitionData(p)
      val future = channel.offer(message)
      context.pipeToSelf(future)(WrappedQueueOfferResult.fromTry(message))
      waitForResult()

    case SendFullPartition(key) =>
      partitions.getSingletonPartition(key) match {
        case Some(p) =>
          val message = FullPartitionData(key, p)
          val future = channel.offer(message)
          context.pipeToSelf(future)(WrappedQueueOfferResult.fromTry(message))
          waitForResult()
        case None =>
          throw new RuntimeException(s"Full partition for key $key not found!")
      }

    case m: WrappedQueueOfferResult =>
      // should not happen
      context.log.warn("Received unexpected queue result: {}", m)
      Behaviors.same
  }

  private def waitForResult(): Behavior[Command] = Behaviors.receiveMessage {
    case WrappedQueueOfferResult(QueueOfferResult.Enqueued, m) =>
      context.log.trace("Successfully enqueued DataMessage: {}", m.getClass.getSimpleName)
      stash.unstash(
        start(), numberOfMessages = 1, wrap = identity
      )
    case WrappedQueueOfferResult(QueueOfferResult.Dropped, m) =>
      context.log.error("Enqueueing data message {} failed: dropped", m.getClass.getSimpleName)
      Behaviors.stopped
    case WrappedQueueOfferResult(QueueOfferResult.Failure(cause), m) =>
      context.log.error("Enqueueing data message {} failed: {}", m.getClass.getSimpleName, cause)
      Behaviors.stopped
    case WrappedQueueOfferResult(QueueOfferResult.QueueClosed, m) =>
      context.log.warn("Channel closed, stopping endpoint. Data message {} not sent!", m.getClass.getSimpleName)
      Behaviors.stopped
    case m =>
      context.log.trace("Stashing {}", m)
      stash.stash(m)
      Behaviors.same
  }

  private def openChannel(): SourceQueueWithComplete[DataMessage] = {
    val (queue, sourceRef) = StreamChannel.prepareSourceRef(context.system)
    remote ! ConnectionOpened(context.self, sourceRef)
    queue
  }
}
