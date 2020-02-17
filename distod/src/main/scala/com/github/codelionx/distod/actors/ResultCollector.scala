package com.github.codelionx.distod.actors

import java.io.{BufferedWriter, FileWriter}

import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{AckBatch, DependencyBatch, ResultCommand, SetAttributeNames}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.types.{OrderDependency, PendingJobMap}

import scala.collection.immutable.HashSet


object ResultCollector {

  val CollectorServiceKey: ServiceKey[ResultCommand] = ServiceKey("resultCollector")

  val name = "result-collector"

  def apply(): Behavior[ResultCommand] = Behaviors.setup { context =>
    new ResultCollector(context).start()
  }

  implicit class WasSeenJobMap(val jobMap: PendingJobMap[ActorRef[_], Int]) extends AnyVal {

    def wasSeen(actorRef: ActorRef[_], id: Int): Boolean =
      jobMap.get(actorRef).fold(false)(_.contains(id))

    def wasNotSeen(actorRef: ActorRef[_], id: Int): Boolean = !wasSeen(actorRef, id)
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
    initialize(HashSet.empty, PendingJobMap.empty)
  }

  private def initialize(
      buffer: Set[OrderDependency],
      seenBatches: PendingJobMap[ActorRef[_], Int]
  ): Behavior[ResultCommand] = Behaviors.receiveMessage {
    case DependencyBatch(id, _, ackTo) if seenBatches.wasSeen(ackTo, id) =>
      context.log.warn("Ignoring duplicated batch {} from {}", id, ackTo)
      ackTo ! AckBatch(id)
      Behaviors.same

    case DependencyBatch(id, deps, ackTo) if seenBatches.wasNotSeen(ackTo, id) =>
      val newBuffer = buffer ++ deps
      val newSeenBatches = seenBatches + (ackTo -> id)
      ackTo ! AckBatch(id)
      initialize(newBuffer, newSeenBatches)

    case SetAttributeNames(attributes) =>
      val writer = createWriter(append = false)
      writeBatch(buffer.toSeq, writer, attributes)
      // ODs are collected in Set (this ensure uniqueness), so we have seen the buffered ODs already
      behavior(attributes, writer, seenBatches, buffer)
  }

  private def behavior(
      attributes: Seq[String],
      writer: BufferedWriter,
      seenBatches: PendingJobMap[ActorRef[_], Int],
      seenODs: Set[OrderDependency]
  ): Behavior[ResultCommand] = Behaviors
    .receiveMessage[ResultCommand] {
      case DependencyBatch(id, _, ackTo) if seenBatches.wasSeen(ackTo, id) =>
        context.log.warn("Ignoring duplicated batch {} from {}", id, ackTo)
        ackTo ! AckBatch(id)
        Behaviors.same

      case DependencyBatch(id, deps, ackTo) if seenBatches.wasNotSeen(ackTo, id) =>
        val (seenDeps, newDeps) = deps.partition(seenODs.contains)
        val newSeenODs =
          if (seenDeps.nonEmpty) {
            context.log.warn("Ignoring {} duplicated ODs from {}", seenDeps.size, ackTo)
            seenODs
          } else {
            seenODs ++ newDeps
          }
        writeBatch(newDeps, writer, attributes)
        ackTo ! AckBatch(id)
        behavior(attributes, writer, seenBatches + (ackTo -> id), newSeenODs)

      case SetAttributeNames(newAttributes) if attributes != newAttributes =>
        context.log.warn("Attribute mapping changed during runtime!!")
        behavior(newAttributes, writer, seenBatches, seenODs)

      case SetAttributeNames(newAttributes) if attributes == newAttributes =>
        Behaviors.same
    }
    .receiveSignal { case (_, PostStop) =>
      cleanup(Some(writer))
      val (fds, ods) = seenODs.partition {
        case _: OrderDependency.ConstantOrderDependency => true
        case _: OrderDependency.EquivalencyOrderDependency => false
      }
      println(s"# FDs: ${fds.size}")
      println(s"# ODs: ${ods.size}")
      Behaviors.stopped
    }
}
