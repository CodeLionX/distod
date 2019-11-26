package com.github.codelionx.distod.actors

import akka.NotUsed
import akka.actor.typed.{Behavior, Terminated}
import akka.actor.typed.scaladsl.Behaviors


object FollowerGuardian {

  def apply(): Behavior[NotUsed] = Behaviors.setup { context =>

    val clusterTester = context.spawn[Nothing](ClusterTester(), ClusterTester.name)
    context.watch(clusterTester)

    Behaviors.receiveSignal {
      case (context, Terminated(ref)) =>
        context.log.info(s"$ref has stopped working!")
        Behaviors.stopped
    }
  }
}
