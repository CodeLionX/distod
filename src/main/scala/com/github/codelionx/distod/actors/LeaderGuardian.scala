package com.github.codelionx.distod.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{Behavior, Terminated}
import com.github.codelionx.distod.partitions.FullPartition


object LeaderGuardian {

  sealed trait Command

  final case class DataLoaded(name: String, headers: Array[String], partitions: Array[FullPartition]) extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>

    context.log.info("LeaderGuardian started, spawning actors ...")
    val clusterTester = context.spawn[Nothing](ClusterTester(), ClusterTester.name)
    context.watch(clusterTester)

    val dataReader = context.spawn(DataReader(context.self), DataReader.name)
    context.watch(dataReader)

    context.log.info("actors started, waiting for data")

    Behaviors
      .receiveMessage[Command] {
        case DataLoaded(name, headers, partitions) =>
          context.log.info("Finished loading dataset {}", name)
          println(s"Table headers: ${headers.mkString(",")}")
          println(partitions.map(p =>
            s"Partition(numberClasses=${p.numberClasses},numberElements=${p.numberElements})"
          ).mkString("\n"))
          Behaviors.stopped
      }
      .receiveSignal {
        case (context, Terminated(ref)) =>
          context.log.info(s"$ref has stopped working!")
          Behaviors.stopped
      }
  }
}
