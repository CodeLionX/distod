package com.github.codelionx.distod.actors

import java.io.{BufferedWriter, FileWriter}

import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.ActorPath
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{AckBatch, DependencyBatch, ResultCommand, SetAttributeNames}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.types.{OrderDependency, PendingJobMap}


object ResultCollector {

  val CollectorServiceKey: ServiceKey[ResultCommand] = ServiceKey("resultCollector")

  val name = "result-collector"

  def apply(): Behavior[ResultCommand] = Behaviors.setup { context =>
    new ResultCollector(context).start()
  }

  implicit class WasSeenJobMap(val jobMap: PendingJobMap[ActorPath, Int]) extends AnyVal {

    def wasSeen(actorPath: ActorPath, id: Int): Boolean =
      jobMap.get(actorPath).fold(false)(_.contains(id))

    def wasNotSeen(actorPath: ActorPath, id: Int): Boolean = !wasSeen(actorPath, id)
  }
}


class ResultCollector(context: ActorContext[ResultCommand]) {

  import ResultCollector._


  private val settings: Settings = Settings(context.system)

  protected def createWriter(append: Boolean): BufferedWriter =
    new BufferedWriter(new FileWriter(settings.outputFilePath, append))

  private def cleanup(writer: Option[BufferedWriter]): Unit = writer match {
    case Some(writer) =>
      try {
        writer.flush()
        writer.close()
      } catch {
        case e: java.io.IOException =>
          context.log.error("Could not flush and close file writer, because {}", e)
      }
    case None => // do nothing
  }

  private def writeBatch(deps: Seq[OrderDependency], writer: BufferedWriter, attributes: Seq[String]): Unit = {
    try {
      for (dep <- deps) {
        val namedDep = dep.withAttributeNames(attributes)
        writer.write(namedDep.toString)
        writer.write("\n")
        if (settings.outputToConsole)
          println(s"Found OD: $namedDep")
      }
      writer.flush()
    } catch {
      case e: java.io.IOException =>
        context.log.error("Writing results failed, because {}!", e.getMessage)
        // escalate!
        throw new RuntimeException("Could not write results to disk!", e)
    }
  }

  def start(): Behavior[ResultCommand] = {
    // register at the registry
    context.system.receptionist ! Receptionist.Register(ResultCollector.CollectorServiceKey, context.self)

    // wait for attribute names before writing results to file (& console)
    initialize(Seq.empty, PendingJobMap.empty)
  }

  private def initialize(
      buffer: Seq[OrderDependency], seenBatches: PendingJobMap[ActorPath, Int]
  ): Behavior[ResultCommand] = Behaviors.receiveMessage {
    case DependencyBatch(id, _, ackTo) if seenBatches.wasSeen(ackTo.path, id) =>
      context.log.warn("Ignoring duplicated batch {} from {}", id, ackTo)
      ackTo ! AckBatch(id)
      Behaviors.same

    case DependencyBatch(id, deps, ackTo) if seenBatches.wasNotSeen(ackTo.path, id) =>
      val newBuffer = buffer ++ deps
      val newSeen = seenBatches + (ackTo.path -> id)
      ackTo ! AckBatch(id)
      initialize(newBuffer, newSeen)

    case SetAttributeNames(attributes) =>
      val writer = createWriter(append = false)
      writeBatch(buffer, writer, attributes)
      behavior(attributes, writer, seenBatches)
  }

  private def behavior(
      attributes: Seq[String], writer: BufferedWriter, seenBatches: PendingJobMap[ActorPath, Int]
  ): Behavior[ResultCommand] = Behaviors
    .receiveMessage[ResultCommand] {
      case DependencyBatch(id, _, ackTo) if seenBatches.wasSeen(ackTo.path, id) =>
        context.log.warn("Ignoring duplicated batch {} from {}", id, ackTo)
        ackTo ! AckBatch(id)
        Behaviors.same

      case DependencyBatch(id, deps, ackTo) if seenBatches.wasNotSeen(ackTo.path, id) =>
        writeBatch(deps, writer, attributes)
        ackTo ! AckBatch(id)
        behavior(attributes, writer, seenBatches + (ackTo.path -> id))

      case SetAttributeNames(newAttributes) if attributes != newAttributes =>
        context.log.warn("Attribute mapping changed during runtime!!")
        behavior(newAttributes, writer, seenBatches)

      case SetAttributeNames(newAttributes) if attributes == newAttributes =>
        Behaviors.same
    }
    .receiveSignal { case (_, PostStop) =>
      cleanup(Some(writer))
      Behaviors.stopped
    }
}