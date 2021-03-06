package com.github.codelionx.distod

import com.github.codelionx.distod.actors.{FollowerGuardian, LeaderGuardian}
import com.typesafe.config.Config


object Main {

  def main(args: Array[String]): Unit = {
    if (systemIsLeader(ActorSystem.defaultConfiguration)) {
      ActorSystem.create(LeaderGuardian())
    } else {
      ActorSystem.create(FollowerGuardian())
    }
  }

  private def systemIsLeader(config: Config): Boolean = {
    val settings = Settings.fromConfig(config)
    settings.actorSystemRole == ActorSystem.LEADER
  }
}
