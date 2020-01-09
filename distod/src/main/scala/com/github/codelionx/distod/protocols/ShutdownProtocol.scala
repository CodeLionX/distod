package com.github.codelionx.distod.protocols

import akka.actor.typed.ActorRef
import com.github.codelionx.distod.Serialization.CborSerializable


object ShutdownProtocol {

  trait ShutdownCommand
  case class PerformShutdown(replyTo: ActorRef[ShutdownCoordinatorCommand]) extends ShutdownCommand with CborSerializable

  trait ShutdownCoordinatorCommand
  case object AlgorithmFinished extends ShutdownCoordinatorCommand
  final case class ShutdownPerformed(ref: ActorRef[ShutdownCommand]) extends ShutdownCoordinatorCommand with CborSerializable
}
