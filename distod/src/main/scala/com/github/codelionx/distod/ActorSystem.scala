package com.github.codelionx.distod

import akka.actor.typed.Behavior
import com.typesafe.config.{Config, ConfigFactory}


/**
 * Factory for our application's actor system
 */
object ActorSystem {

  sealed trait Role

  /**
   * Marks a node as a leader node.
   *
   * @see [[com.github.codelionx.distod.Settings#actorSystemRole]]
   */
  case object LEADER extends Role


  /**
   * Marks a node as a follower node.
   *
   * @see [[com.github.codelionx.distod.Settings#actorSystemRole]]
   */
  case object FOLLOWER extends Role

  val distodVersion = Version.current
  val distodGitSHA = Version.gitSHA

  def defaultConfiguration: Config = ConfigFactory.load()

  def create[T](userGuardian: Behavior[T]): akka.actor.typed.ActorSystem[T] = {
    val actorSystemName = defaultConfiguration.getString("distod.system-name")

    val versionInfoMsg = s"DISTOD version: $distodVersion\nDISTOD commit: $distodGitSHA"

    val system = create(actorSystemName, defaultConfiguration, userGuardian)
    println(versionInfoMsg)
    system.log.info(versionInfoMsg)
    system
  }

  def create[T](actorSystemName: String, config: Config, userGuardian: Behavior[T]): akka.actor.typed.ActorSystem[T] =
    akka.actor.typed.ActorSystem(userGuardian, actorSystemName, config)


}
