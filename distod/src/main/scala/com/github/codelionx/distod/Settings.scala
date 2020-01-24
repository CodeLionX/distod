package com.github.codelionx.distod

import java.util.concurrent.TimeUnit

import akka.actor.typed.{ActorSystem, DispatcherSelector, Extension, ExtensionId}
import com.github.codelionx.distod.ActorSystem.{FOLLOWER, LEADER, Role}
import com.github.codelionx.distod.Settings.InputParsingSettings
import com.typesafe.config.{Config, ConfigException}

import scala.concurrent.duration.FiniteDuration


/**
 * @see [[com.github.codelionx.distod.Settings]]
 */
object Settings extends ExtensionId[Settings] {

  override def createExtension(system: ActorSystem[_]): Settings = new Settings(system.settings.config)

  def fromConfig(config: Config): Settings = new Settings(config)

  trait InputParsingSettings {

    def filePath: String

    def hasHeader: Boolean

    def maxColumns: Option[Int]

    def maxRows: Option[Int]
  }

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

  def rawConfig: Config = config

  val actorSystemName: String = config.getString(s"$namespace.system-name")

  val actorSystemRole: Role = config.getString(s"$namespace.system-role") match {
    case "leader" => LEADER
    case "follower" => FOLLOWER
    case s => throw new ConfigException.BadValue(
      s"$namespace.system-role",
      s"$s is not allowed, use either 'leader' or 'follower'"
    )
  }

  val outputFilePath: String = config.getString(s"$namespace.output-file")

  val outputToConsole: Boolean = config.getBoolean(s"$namespace.output-to-console")

  val resultBatchSize: Int = config.getInt(s"$namespace.result-batch-size")

  val host: String = config.getString(s"$namespace.host")
  val port: Int = config.getInt(s"$namespace.port")

  val leaderHost: String = config.getString(s"$namespace.leader-host")
  val leaderPort: Int = config.getInt(s"$namespace.leader-port")

  val maxWorkers: Int = config.getInt(s"$namespace.max-workers")

  val numberOfWorkers: Int = {
    val cores = Runtime.getRuntime.availableProcessors()
    scala.math.min(maxWorkers, cores)
  }

  val cpuBoundTaskDispatcher: DispatcherSelector =
    DispatcherSelector.fromConfig(s"$namespace.cpu-bound-tasks-dispatcher")

  // cuts off nanosecond part of durations (we dont care about this, because duration should be in
  // seconds or greater anyway
  val partitionManagerCleanupInterval: FiniteDuration = FiniteDuration.apply(
    config.getDuration(s"$namespace.partition-manager.cleanup-interval").getSeconds,
    TimeUnit.SECONDS
  )

  val inputParsingSettings: InputParsingSettings = new InputParsingSettings {

    private val subnamespace = s"$namespace.input"

    val filePath: String = config.getString(s"$subnamespace.path")

    val hasHeader: Boolean = config.getBoolean(s"$subnamespace.has-header")

    val maxColumns: Option[Int] = if (config.hasPath(s"$subnamespace.max-columns"))
      Some(config.getInt(s"$subnamespace.max-columns"))
    else
      None

    val maxRows: Option[Int] = if (config.hasPath(s"$subnamespace.max-rows"))
      Some(config.getInt(s"$subnamespace.max-rows"))
    else
      None
  }
}
