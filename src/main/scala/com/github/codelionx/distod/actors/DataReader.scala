package com.github.codelionx.distod.actors

import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.LeaderGuardian.DataLoaded
import com.github.codelionx.distod.io.CSVParser
import com.github.codelionx.distod.partitions.{FullPartition, Partition}


object DataReader {

  val name = "data-reader"

  sealed trait Command extends CborSerializable

  private final case class PartitioningFinished(columnId: Int, partition: FullPartition) extends Command

  def apply(replyTo: ActorRef[DataLoaded]): Behavior[Command] = Behaviors.setup { context =>
    new DataReader(context, replyTo).start()
  }

  def partitioner(columnId: Int, column: Array[String], replyTo: ActorRef[PartitioningFinished]): Behavior[NotUsed] =
    Behaviors.setup { context =>
      context.log.info("Partitioning column {}", columnId)
      val partition = Partition.fullFrom(column)
      replyTo ! PartitioningFinished(columnId, partition)
      Behaviors.stopped
    }
}


class DataReader(
                  context: ActorContext[DataReader.Command],
                  replyTo: ActorRef[DataLoaded]
                ) {

  import DataReader._


  private val settings = Settings(context.system)
  private val parser = CSVParser(settings)

  context.log.info("DataReader started, parsing data from {}", settings.inputParsingSettings.filePath)
  private val table = parser.parse()

  private def start(): Behavior[Command] = {
    // send columns out to temp actors
    table.columns.zipWithIndex.foreach { case (column, id) =>
      context.spawnAnonymous(partitioner(id, column, context.self))
    }

    // collect results
    collectPartitions(Map.empty, table.columns.length)
  }

  private def collectPartitions(partitions: Map[Int, FullPartition], expected: Int): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case PartitioningFinished(columnId, partition) =>
        val newPartitions = partitions.updated(columnId, partition)
        context.log.info("Received partition for column {}, ({}/{})", columnId, newPartitions.size, expected)
        if (newPartitions.size == expected) {
          replyTo ! DataLoaded(table.name, table.headers, partitions.toSeq.sortBy(_._1).map(_._2).toArray)
          Behaviors.stopped
        } else {
          collectPartitions(newPartitions, expected)
        }
    }
}