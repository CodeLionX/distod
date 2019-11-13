package com.github.codelionx.distod

import akka.actor.typed.{ActorSystem, Extension, ExtensionId}
import com.github.codelionx.distod.ActorSystem.{FOLLOWER, LEADER, Role}
import com.typesafe.config.{Config, ConfigException}


/**
 * @see [[com.github.codelionx.distod.Settings]]
 */
object Settings extends ExtensionId[Settings] {

  override def createExtension(system: ActorSystem[_]): Settings = new Settings(system.settings.config)

}


/**
 * Bundles access to our application specific settings keys. Use like :
 *
 * @example {{{
 * val settings = Settings(context.system)
 * println(settings.actorSystemRole)
 * }}}
 */
class Settings private(config: Config) extends Extension {

  private val namespace = "distod"

  val actorSystemName: String = config.getString(s"$namespace.system-name")

  val actorSystemRole: Role = config.getString(s"$namespace.system-role") match {
    case "leader" => LEADER
    case "follower" => FOLLOWER
    case s => throw new ConfigException.BadValue(
      s"$namespace.system-role",
      s"$s is not allowed, use either 'leader' or 'follower'"
    )
  }

  val inputFilePath: String = config.getString(s"$namespace.input-file")

  val inputFileHasHeader: Boolean = config.getBoolean(s"$namespace.input-has-header")

  val outputFilePath: String = config.getString(s"$namespace.output-file")

  val outputToConsole: Boolean = config.getBoolean(s"$namespace.output-to-console")

  val host: String = config.getString(s"$namespace.host")
  val port: Int = config.getInt(s"$namespace.port")

  val leaderHost: String = config.getString(s"$namespace.leader-host")
  val leaderPort: Int = config.getInt(s"$namespace.leader-port")

  //  val workers: Int = config.getInt(s"$namespace.workers")

  //  val maxBatchSize: Int = config.getInt(s"$namespace.max-batch-size")

}
