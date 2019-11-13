package com.github.codelionx.distod

import akka.NotUsed
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.ClusterTester


object Main {

  def main(args: Array[String]): Unit = {
    ActorSystem.create[NotUsed](userGuardian)
  }

  // TODO: if the user guardian actor gets larger: extract it to own object
  def userGuardian: Behavior[NotUsed] = Behaviors.setup { context =>
    val settings = Settings(context.system)

    println(
      s"""
         |Settings configuration:
         | '${settings.actorSystemName}' actor system with role ${settings.actorSystemRole}
         | hostname: ${settings.host}
         | port: ${settings.port}
         | Leader is at: ${settings.leaderHost}:${settings.leaderPort}
         | ${if (settings.actorSystemRole == ActorSystem.LEADER) s"Reading data from ${settings.inputFilePath}" else ""}
         |""".stripMargin
    )

    context.spawn[Nothing](ClusterTester(), ClusterTester.name)

    Behaviors.empty
  }
}
