package com.github.codelionx.distod.actors

import akka.actor.CoordinatedShutdown
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.protocols.ShutdownProtocol.{PerformShutdown, ShutdownCommand}


object Executioner {

  val ExecutionerServiceKey: ServiceKey[ShutdownCommand] = ServiceKey("executioner")

  val name = "executioner"

  def apply(): Behavior[ShutdownCommand] = Behaviors.setup { context =>
    // register at the registry
    context.system.receptionist ! Receptionist.Register(Executioner.ExecutionerServiceKey, context.self)

    Behaviors.receiveMessage {
      case PerformShutdown(reasonHolder) =>
        context.log.info("Received request to perform shut down! Reason: {}", reasonHolder.reason)
        CoordinatedShutdown(context.system).run(reasonHolder.reason)
        Behaviors.same
    }
  }
}
