package com.github.codelionx.distod.protocols

import akka.actor.typed.ActorRef
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.types.ShutdownReason


object ShutdownProtocol {

  trait ShutdownCommand
  case class PerformShutdown(reason: ShutdownReason) extends ShutdownCommand with CborSerializable

  trait ShutdownCoordinatorCommand
  case object AlgorithmFinished extends ShutdownCoordinatorCommand
  case class ForceFollowerShutdown(replyTo: ActorRef[ForcedFollowerShutdownComplete.type])
    extends ShutdownCoordinatorCommand

  case object ForcedFollowerShutdownComplete
}
