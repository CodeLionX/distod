package com.github.codelionx.distod.actors.worker

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.ActorRef
import com.github.codelionx.distod.actors.master.{Master, MasterHelper}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.{PartitionCommand, PartitionEvent}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand


case class WorkerContext(
    context: ActorContext[Worker.Command],
    master: ActorRef[MasterHelper.Command],
    partitionManager: ActorRef[PartitionCommand],
    rsProxy: ActorRef[ResultProxyCommand],
    partitionEventMapper: ActorRef[PartitionEvent]
)
