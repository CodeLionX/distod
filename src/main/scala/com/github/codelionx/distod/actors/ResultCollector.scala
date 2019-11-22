package com.github.codelionx.distod.actors

import java.io.{BufferedWriter, FileWriter}

import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.ActorPath
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{AckBatch, DependencyBatch, ResultCommand}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.types.{OrderDependency, PendingJobMap}


object ResultCollector {

  val CollectorServiceKey: ServiceKey[ResultCommand] = ServiceKey("resultCollector")

  val name = "result-collector"

  def apply(): Behavior[ResultCommand] = Behaviors.setup { context =>
    new ResultCollector(context).start()
  }
}


class ResultCollector(context: ActorContext[ResultCommand]) {

  private val settings = Settings(context.system)

  private def recreateWriter(currentWriter: Option[BufferedWriter], append: Boolean): BufferedWriter = {
    cleanup(currentWriter)
    new BufferedWriter(new FileWriter(settings.outputFilePath, append))
  }

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

  @throws[java.io.IOException]
  private def writeBatch(deps: Seq[OrderDependency], writer: BufferedWriter): Unit = {
    for (dep <- deps) {
      writer.write(dep.toString)
      writer.write("\n")
      if (settings.outputToConsole)
        context.log.info("Found OD: {}", dep)
    }
    writer.flush()
  }

  def start(): Behavior[ResultCommand] = {
    // register at the registry
    context.system.receptionist ! Receptionist.Register(ResultCollector.CollectorServiceKey, context.self)

    behavior(recreateWriter(None, append = false), PendingJobMap.empty)
  }

  private def behavior(
      writer: BufferedWriter, seenBatches: PendingJobMap[ActorPath, Int]
  ): Behavior[ResultCommand] = Behaviors
    .receiveMessage { case DependencyBatch(id, deps, ackTo) =>
      val wasSeen = seenBatches.get(ackTo.path).fold(false)(_.contains(id))
      if (wasSeen) {
        context.log.warn("Ignoring duplicated batch {} from {}", id, ackTo)
        Behaviors.same
      } else {
        try {
          writeBatch(deps, writer)
          ackTo ! AckBatch(id)
          behavior(writer, seenBatches + (ackTo.path -> id))
        } catch {
          case e: java.io.IOException =>
            context.log.debug("Writing results failed, because {}! Recreating file writer ...", e.getMessage)
            // do not wrap in try to let a potential second exception stop the actor
            val newWriter = recreateWriter(Some(writer), append = true)
            writeBatch(deps, newWriter)
            behavior(newWriter, seenBatches + (ackTo.path -> id))
        }
      }
    }
    .receiveSignal { case (_, PostStop) =>
      cleanup(Some(writer))
      Behaviors.stopped
    }
}