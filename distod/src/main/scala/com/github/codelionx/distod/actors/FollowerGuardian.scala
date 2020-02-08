package com.github.codelionx.distod.actors

import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, Terminated}
import com.github.codelionx.distod.actors.partitionMgmt.{PartitionManager, PartitionReplicator}
import com.github.codelionx.distod.actors.worker.WorkerManager
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand


object FollowerGuardian {

  sealed trait Command
  case object Shutdown extends Command


  def apply(): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("FollowerGuardian started, spawning actors ...")

    val partitionManager = startFollowerActors(context)

    // executioner stops the actor system when the algorithm is finished
    // needs to flush the result collector beforehand
    val executioner = context.spawn(Executioner(), Executioner.name)
    context.watch(executioner)

    // will terminate after successful replication
    context.spawn(PartitionReplicator(partitionManager), PartitionReplicator.name)

    Behaviors.receiveSignal {
      case (context, Terminated(ref)) =>
        if(CoordinatedShutdown(context.system).shutdownReason().isEmpty) {
          context.log.warn("{} has stopped working!", ref)
        }
        Behaviors.stopped
    }
  }

  def startFollowerActors[T](context: ActorContext[T]): ActorRef[PartitionCommand] = {
//    val clusterTester = context.spawn[Nothing](ClusterTester(), ClusterTester.name)
//    context.watch(clusterTester)

    // system montior
    val monitor = context.spawn(SystemMonitor(), SystemMonitor.name)

    // local partition manager
    val partitionManager = context.spawn(PartitionManager(monitor), PartitionManager.name)
    context.watch(partitionManager)

    // local result collector proxy
    val rsProxy = context.spawn(ResultCollectorProxy(), ResultCollectorProxy.name)
    context.watch(rsProxy)

    // local worker manager spawns the workers
    val workerManager = context.spawn(WorkerManager(partitionManager, rsProxy), WorkerManager.name)
    context.watch(workerManager)

    partitionManager
  }
}
