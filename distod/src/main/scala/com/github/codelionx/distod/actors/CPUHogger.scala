package com.github.codelionx.distod.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors


object CPUHogger {

  case object Ping

  def apply(id: Int): Behavior[Ping.type] = Behaviors.setup { context =>
    context.log.info("Started hogger {}", id)
    context.self ! Ping

    Behaviors.receiveMessage { _ =>
      var x: Long = 0
      while (x < Long.MaxValue / 1000000000) {
        x += 1
      }
      context.log.info("Hogger {} finished a round", id)
      context.self ! Ping
      Behaviors.same
    }
  }
}
