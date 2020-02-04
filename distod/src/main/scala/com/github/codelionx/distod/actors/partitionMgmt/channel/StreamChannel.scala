package com.github.codelionx.distod.actors.partitionMgmt.channel

import akka.actor.typed.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.NotUsed
import com.github.codelionx.distod.Settings


object StreamChannel {

  private val terminationMarker = ByteString(Seq(0, 0, 0, 0, 1, 1, 1, 1).map(_.toByte))

  def prepareSourceRef(system: ActorSystem[_]): (SourceQueueWithComplete[DataMessage], SourceRef[ByteString]) = {
    implicit val _system: ActorSystem[_] = system
    implicit val mat: Materializer = SystemMaterializer(system).materializer

    val source = Source
      .queue[DataMessage](10, OverflowStrategy.backpressure)
      .map(DataMessage.serialize)
      .map(_ ++ terminationMarker)
      .flatMapConcat(bytes => Source.fromIterator(() => bytes.grouped(200000)))
    val graph = source.toMat(StreamRefs.sourceRef())(Keep.both)
    graph.run()
  }

  def consumeSourceRefWith[T](
      sourceRef: SourceRef[ByteString], sink: Sink[DataMessage, T], system: ActorSystem[_]
  ): T = {
    implicit val _system: ActorSystem[_] = system
    implicit val mat: Materializer = SystemMaterializer(system).materializer

    sourceRef
      .via(Framing.delimiter(terminationMarker, maximumFrameLength = 200000000, allowTruncation = true))
      .wireTap(s => println(s"Size: ${s.size}"))
      .via(parallelDeserializer(Settings(system).numberOfWorkers))
//      .map(DataMessage.deserialize)
      .runWith(sink)
  }

  // see: https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html#cookbook-balance
  private def parallelDeserializer(parallelism: Int)
    (implicit system: ActorSystem[_]): Flow[ByteString, DataMessage, NotUsed] = {
    import GraphDSL.Implicits._

    val job = Flow.fromFunction(DataMessage.deserialize)

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[ByteString](parallelism, waitForAllDownstreams = true))
      val merger = b.add(Merge[DataMessage](parallelism))

      for (_ <- 0 until parallelism) {
        balancer ~> job.async ~> merger
      }

      FlowShape(balancer.in, merger.out)
    })
  }
}
