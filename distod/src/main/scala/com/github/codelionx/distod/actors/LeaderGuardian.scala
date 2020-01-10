package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior, Terminated}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.master.Master
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{FlushAndStop, FlushFinished}
import com.github.codelionx.distod.protocols.ShutdownProtocol


object LeaderGuardian {

  sealed trait Command
  case object AlgorithmFinished extends Command
  case object Shutdown extends Command
  case class WrappedRSProxyEvent(event: FlushFinished.type) extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    val rsAdapter = context.messageAdapter(WrappedRSProxyEvent)
    context.log.info("LeaderGuardian started, spawning actors ...")

    // spawn follower actors for local computation
    val (partitionManager, rsProxy) = FollowerGuardian.startFollowerActors(context)

    // shutdown coordinator first stops the followers and then the leader when algorithm has finished
    val shutdownCoordinator = context.spawn(ShutdownCoordinator(context.self), ShutdownCoordinator.name)
    context.watch(shutdownCoordinator)

    val timeBeforeStart = System.nanoTime()

    // spawn leader actors
    startLeaderActors(context, partitionManager)

    context.log.debug("Actors started, algorithm is running")

    Behaviors
      .receiveMessage[Command] {
        case AlgorithmFinished =>
          println(s"Runtime (ms): ${((System.nanoTime() - timeBeforeStart) / 1e6).toInt}")
          shutdownCoordinator ! ShutdownProtocol.AlgorithmFinished
          Behaviors.same

        case Shutdown =>
          rsProxy ! FlushAndStop(rsAdapter)
          Behaviors.same

        case WrappedRSProxyEvent(FlushFinished) =>
          context.log.info("Stopping leader node!")
          context.unwatch(rsProxy)
          Behaviors.stopped
      }
      .receiveSignal {
        case (context, Terminated(ref)) =>
          context.log.warn("{} has stopped working!", ref)
          if (context.children.isEmpty) {
            context.log.error("There are no child actors left. Shutting down system.")
            Behaviors.stopped
          } else {
            Behaviors.same
          }
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
    val master = context.spawn(Master(context.self, dataReader, partitionManager, rs), Master.name)
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
