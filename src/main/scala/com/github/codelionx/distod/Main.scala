package com.github.codelionx.distod

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{Behavior, Terminated}
import com.github.codelionx.distod.actors.{ClusterTester, DataReader}


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
         | ${if (settings.actorSystemRole == ActorSystem.LEADER) s"Reading data from ${settings.inputParsingSettings.filePath}" else ""}
         |""".stripMargin
    )

    val clusterTester = context.spawn[Nothing](ClusterTester(), ClusterTester.name)
    context.watch(clusterTester)

    if(settings.actorSystemRole == ActorSystem.LEADER) {
      val dataReader = context.spawn(DataReader(), DataReader.name)
      context.watch(dataReader)
    }

    Behaviors.receiveSignal{
      case (context, Terminated(ref)) =>
        context.log.info(s"$ref has stopped working!")
        Behaviors.stopped
    }
  }
}
