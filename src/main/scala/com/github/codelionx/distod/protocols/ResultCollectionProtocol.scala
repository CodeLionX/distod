package com.github.codelionx.distod.protocols

import akka.actor.typed.ActorRef
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.types.OrderDependency


object ResultCollectionProtocol {

  sealed trait ResultCommand extends CborSerializable
  final case class DependencyBatch(id: Int, deps: Seq[OrderDependency], ackTo: ActorRef[AckBatch])
    extends ResultCommand

  trait ResultProxyCommand extends CborSerializable
  final case class AckBatch(id: Int) extends ResultProxyCommand
  final case class FoundDependencies(deps: Seq[OrderDependency]) extends ResultProxyCommand
  final case class FlushAndStop(replyTo: ActorRef[FlushFinished.type]) extends ResultProxyCommand

  case object FlushFinished
}
