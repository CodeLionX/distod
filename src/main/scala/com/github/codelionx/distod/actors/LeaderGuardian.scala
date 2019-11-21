package com.github.codelionx.distod.actors

import akka.actor.typed.{Behavior, Terminated}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.Settings


object LeaderGuardian {

  sealed trait Command
  case object AlgorithmFinished extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("LeaderGuardian started, spawning actors ...")

    context.spawn[Nothing](ClusterTester(), ClusterTester.name)
    // testCPUhogging(context)

    // temp. data reader
    val dataReader = context.spawn(DataReader(), DataReader.name)
    // don't watch data reader, because it will be shut down after initialization

    // local partition manager
    val partitionManager = context.spawn(PartitionManager(), PartitionManager.name)
    context.watch(partitionManager)

    // master is only spawned by leader
    val master = context.spawn(Master(context.self, dataReader, partitionManager), Master.name)
    context.watch(master)

    // worker manager spawns the workers
    val workerManager = context.spawn(WorkerManager(partitionManager), WorkerManager.name)
    context.watch(workerManager)

    context.log.info("Actors started, waiting for data")


    Behaviors
      .receiveMessage[Command] {
        case AlgorithmFinished =>
          context.log.info("Received message that algorithm has finished successfully. Shutting down system.")
          Behaviors.stopped
      }
      .receiveSignal {
        case (context, Terminated(ref)) =>
          context.log.info("{} has stopped working!", ref)
          if (context.children.isEmpty) {
            context.log.info("There are no child actors left. Shutting down system.")
            Behaviors.stopped
          } else {
            Behaviors.same
          }
      }
  }

  private def testCPUhogging(context: ActorContext[Command]): Unit = {
    val settings = Settings(context.system)
    val n = 16
    context.log.info("Starting {} hoggers", n)
    (0 until n).foreach { i =>
      val ref = context.spawnAnonymous(CPUHogger(i), settings.cpuBoundTaskDispatcher)
      context.watch(ref)
    }
  }
}
