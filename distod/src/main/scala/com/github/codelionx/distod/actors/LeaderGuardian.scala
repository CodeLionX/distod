package com.github.codelionx.distod.actors

import akka.actor.CoordinatedShutdown
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector, Terminated}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.master.Master
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand
import com.github.codelionx.distod.protocols.ShutdownProtocol
import com.github.codelionx.distod.types.ShutdownReason
import com.github.codelionx.util.timing.Timing


object LeaderGuardian {

  sealed trait Command
  case object AlgorithmFinished extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("LeaderGuardian started, spawning actors ...")

    // spawn follower actors for local computation
    val partitionManager = FollowerGuardian.startFollowerActors(context)

    // shutdown coordinator first stops the followers and then the leader when algorithm has finished
    val shutdownCoordinator = context.spawn(ShutdownCoordinator(context.self), ShutdownCoordinator.name)
    context.watch(shutdownCoordinator)

    val startNanos = System.nanoTime()

    // spawn leader actors
    startLeaderActors(context, partitionManager)

    context.log.debug("Actors started, algorithm is running")

    Behaviors
      .receiveMessage[Command] {
        case AlgorithmFinished =>
          val duration = System.nanoTime() - startNanos
          println(s"TIME Overall runtime: ${(duration / 1e6).toLong} ms")
          shutdownCoordinator ! ShutdownProtocol.AlgorithmFinished
          Behaviors.same
      }
      .receiveSignal {
        case (context, Terminated(ref)) =>
          if(CoordinatedShutdown(context.system).shutdownReason().isEmpty) {
            context.log.warn("{} has stopped working!", ref)
          }
          if (context.children.isEmpty) {
            context.log.error("There are no child actors left. Shutting down system.")
            CoordinatedShutdown(context.system).run(ShutdownReason.AllActorsDiedReason)
          }
          Behaviors.same
      }
  }

  def startLeaderActors(context: ActorContext[Command], partitionManager: ActorRef[PartitionCommand]): Unit = {
    // temp. data reader
    val dataReader = context.spawn(DataReader(), DataReader.name)
    // don't watch data reader, because it will be shut down after initialization

    // the single result collector
    val rs = context.spawn(ResultCollector(), ResultCollector.name)
    context.watch(rs)

    // master
    val master = context.spawn(
      Master(context.self, dataReader, partitionManager, rs),
      Master.name,
      DispatcherSelector.fromConfig("distod.master-pinned-dispatcher")
    )
    context.watch(master)
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
