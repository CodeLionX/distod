package com.github.codelionx.distod.actors

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.io.CSVParser
import com.github.codelionx.distod.partitions.{FullPartition, Partition}
import com.github.codelionx.distod.protocols.DataLoadingProtocol.{DataLoadingCommand, LoadPartitions, PartitionsLoaded, Stop}
import com.github.codelionx.distod.types.PartitionedTable


object DataReader {

  val name = "data-reader"

  private final case class PartitioningFinished(columnId: Int, partition: FullPartition) extends DataLoadingCommand

  def apply(): Behavior[DataLoadingCommand] =
    Behaviors.setup(context =>
      Behaviors.withStash(10) { messageBuffer =>
        new DataReader(context, messageBuffer).start()
      }
    )

  def partitioner(columnId: Int, column: Array[String], replyTo: ActorRef[PartitioningFinished]): Behavior[NotUsed] =
    Behaviors.setup { context =>
      context.log.debug("Partitioning column {}", columnId)
      val partition = Partition.fullFrom(column)
      replyTo ! PartitioningFinished(columnId, partition)
      Behaviors.stopped
    }
}


class DataReader(
                  context: ActorContext[DataLoadingCommand],
                  buffer: StashBuffer[DataLoadingCommand]
                ) {

  import DataReader._


  private val settings = Settings(context.system)
  private val parser = CSVParser(settings)

  context.log.info("DataReader started, parsing data from {}", settings.inputParsingSettings.filePath)
  private val table = parser.parse()

  private def start(): Behavior[DataLoadingCommand] = {
    // Load data early:

    // send columns out to temp actors
    table.columns.zipWithIndex.foreach { case (column, id) =>
      context.spawnAnonymous(partitioner(id, column, context.self), settings.cpuBoundTaskDispatcher)
    }

    // collect results
    collectPartitions(Map.empty, table.columns.length)
  }

  private def collectPartitions(partitions: Map[Int, FullPartition], expected: Int): Behavior[DataLoadingCommand] =
    Behaviors.receiveMessage {
      case m: LoadPartitions =>
        context.log.debug("Stashing request to load partitions from {}", m.replyTo)
        buffer.stash(m)
        Behaviors.same

      case Stop =>
        context.log.warn("Data reading and partitioning not finished, but actor was stopped by external request!")
        Behaviors.stopped

      case PartitioningFinished(columnId, partition) =>
        val newPartitions = partitions.updated(columnId, partition)
        context.log.debug("Received partition for column {}, ({}/{})", columnId, newPartitions.size, expected)
        if (newPartitions.size == expected) {
          context.log.info("Data ready")
          val orderedPartitions = newPartitions.toSeq.sortBy(_._1)
          val partitionsArray = orderedPartitions.map(_._2).toArray
          buffer.unstashAll(dataReady(partitionsArray))
        } else {
          collectPartitions(newPartitions, expected)
        }
    }

  private def dataReady(partitions: Array[FullPartition]): Behavior[DataLoadingCommand] =
    Behaviors.receiveMessagePartial {
      case LoadPartitions(replyTo) =>
        replyTo ! PartitionsLoaded(PartitionedTable(
          name = table.name,
          headers = table.headers,
          partitions = partitions
        ))
        Behaviors.same

      case Stop =>
        context.log.info("Data reader was stopped by request.")
        Behaviors.stopped
    }
}