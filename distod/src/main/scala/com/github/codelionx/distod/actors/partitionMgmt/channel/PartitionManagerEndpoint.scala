package com.github.codelionx.distod.actors.partitionMgmt

import java.nio.ByteOrder
import java.util.UUID

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import akka.serialization.{SerializationExtension, Serializers}
import akka.stream._
import akka.stream.scaladsl.{Framing, Keep, Source, SourceQueueWithComplete, StreamRefs}
import akka.util.{ByteString, ByteStringBuilder}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
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

  trait DataMessage extends CborSerializable
  final case class Attributes(attributes: Seq[Int]) extends DataMessage
  final case class EmptyPartition(value: StrippedPartition) extends DataMessage
  final case class FullPartitionFound(key: CandidateSet, value: FullPartition) extends DataMessage

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


  implicit private val mat: Materializer = SystemMaterializer(context.system).materializer
  private val channel = openChannel()

  def start(): Behavior[Command] = Behaviors.receiveMessage {
    case SendAttributes =>
      val message = Attributes(attributes)
      val future = channel.offer(message)
      context.pipeToSelf(future)(WrappedQueueOfferResult.fromTry(message))
      waitForResult()

    case SendEmptyPartition =>
      val p = partitions(CandidateSet.empty)
      val message = EmptyPartition(p)
      val future = channel.offer(message)
      context.pipeToSelf(future)(WrappedQueueOfferResult.fromTry(message))
      waitForResult()

    case SendFullPartition(key) =>
      partitions.getSingletonPartition(key) match {
        case Some(p) =>
          val message = FullPartitionFound(key, p)
          val future = channel.offer(message)
          context.pipeToSelf(future)(WrappedQueueOfferResult.fromTry(FullPartitionFound(key, p)))
          waitForResult()
        case None =>
          throw new RuntimeException(s"Full partition for key $key not found!")
      }
  }

  private def waitForResult(): Behavior[Command] = Behaviors.receiveMessage {
    case WrappedQueueOfferResult(QueueOfferResult.Enqueued, m) =>
      context.log.info("Successfully enqueued DataMessage: {}", m.getClass.getSimpleName)
      stash.unstashAll(
        start()
      )
    case WrappedQueueOfferResult(QueueOfferResult.Dropped, m) =>
      context.log.error("Enqueueing data message {} failed: dropped", m)
      Behaviors.stopped
    case WrappedQueueOfferResult(QueueOfferResult.Failure(cause), m) =>
      context.log.error("Enqueueing data message {} failed: {}", m, cause)
      Behaviors.stopped
    case WrappedQueueOfferResult(QueueOfferResult.QueueClosed, m) =>
      context.log.warn("Channel closed, stopping endpoint. Data message {} not sent!", m)
      Behaviors.stopped
    case m =>
      context.log.info("Stashing {}", m)
      stash.stash(m)
      Behaviors.same
  }

  private def openChannel(): SourceQueueWithComplete[DataMessage] = {
    val source = Source
      .queue[DataMessage](10, OverflowStrategy.backpressure)
      .map(serialize)
      .flatMapConcat(bytes => Source.fromIterator(() => bytes.grouped(200000)))
//      .via(Framing.simpleFramingProtocolEncoder(200000))
    val graph = source.toMat(StreamRefs.sourceRef())(Keep.both)
    val (queue, sourceRef) = graph.run()
    remote ! ConnectionOpened(context.self, sourceRef)
    queue
  }

  private def serialize(m: DataMessage): ByteString = {
    implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
    val serialization = SerializationExtension(context.system)
    val serializer = serialization.findSerializerFor(m)
    val manifest = Serializers.manifestFor(serializer, m)
    val manifestBytes = ByteString(manifest)

    val b = new ByteStringBuilder
    b.putInt(serializer.identifier)
    b.putInt(manifestBytes.size)
    b.append(manifestBytes)
    b.putBytes(serializer.toBinary(m))
    b.result().compact ++ ByteString(0.toByte, 0.toByte, 0.toByte, 0.toByte)
  }
}
