package com.github.codelionx.distod.actors

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior, Terminated}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.actors.partitionMgmt.{PartitionManager, PartitionReplicator}
import com.github.codelionx.distod.actors.worker.WorkerManager
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand


object FollowerGuardian {

  def apply(): Behavior[NotUsed] = Behaviors.setup { context =>
    context.log.info("FollowerGuardian started, spawning actors ...")

    val (partitionManager, _) = startFollowerActors(context)

    val partitionReplicator = context.spawn(PartitionReplicator(partitionManager), PartitionReplicator.name)
    context.watch(partitionReplicator)

    Behaviors.receiveSignal {
      case (context, Terminated(ref)) =>
        context.log.info(s"$ref has stopped working!")
        Behaviors.stopped
    }
  }

  def startFollowerActors[T](context: ActorContext[T]): (ActorRef[PartitionCommand], ActorRef[ResultProxyCommand]) = {
    val clusterTester = context.spawn[Nothing](ClusterTester(), ClusterTester.name)
    context.watch(clusterTester)

    // local partition manager
    val partitionManager = context.spawn(PartitionManager(), PartitionManager.name)
    context.watch(partitionManager)

    // local result collector proxy
    val rsProxy = context.spawn(ResultCollectorProxy(), ResultCollectorProxy.name)
    context.watch(rsProxy)

    // local worker manager spawns the workers
    val workerManager = context.spawn(WorkerManager(partitionManager, rsProxy), WorkerManager.name)
    context.watch(workerManager)

    (partitionManager, rsProxy)
  }
}
