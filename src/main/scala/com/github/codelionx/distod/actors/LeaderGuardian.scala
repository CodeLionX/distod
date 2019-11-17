package com.github.codelionx.distod.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{Behavior, Terminated}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.protocols.DataLoadingProtocol.{DataLoaded, DataLoadingEvent}


object LeaderGuardian {

  sealed trait Command
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    val loadingEventMapper = context.messageAdapter(e => WrappedLoadingEvent(e))

    context.log.info("LeaderGuardian started, spawning actors ...")

    val clusterTester = context.spawn[Nothing](ClusterTester(), ClusterTester.name)
    context.watch(clusterTester)

    val dataReader = context.spawn(DataReader(loadingEventMapper), DataReader.name)
    context.watch(dataReader)

    context.log.info("actors started, waiting for data")

    //    testCPUhogging(context)

    def onLoadingEvent(loadingEvent: DataLoadingEvent): Behavior[Command] = loadingEvent match {
      case DataLoaded(name, headers, partitions) =>
        context.log.info("Finished loading dataset {}", name)
        println(s"Table headers: ${headers.mkString(",")}")
        println(partitions.map(p =>
          s"Partition(numberClasses=${p.numberClasses},numberElements=${p.numberElements})"
        ).mkString("\n"))
        val combined = partitions(2) * partitions(3)
        println(combined.equivClasses.size)

        val combinedStripped = partitions(2).stripped * partitions(3).stripped
        println(combinedStripped.equivClasses.size)
        Behaviors.stopped
    }

    Behaviors
      .receiveMessage[Command] {
        case WrappedLoadingEvent(event) => onLoadingEvent(event)
      }
      .receiveSignal {
        case (context, Terminated(ref)) =>
          context.log.info(s"$ref has stopped working!")
          Behaviors.stopped
      }
  }

  def testCPUhogging(context: ActorContext[Command]): Unit = {
    val settings = Settings(context.system)
    val n = 16
    context.log.info("Starting {} hoggers", n)
    (0 until n).foreach { i =>
      val ref = context.spawnAnonymous(CPUHogger(i), settings.cpuBoundTaskDispatcher)
      context.watch(ref)
    }
  }
}
