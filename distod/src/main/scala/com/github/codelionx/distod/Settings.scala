package com.github.codelionx.distod

import java.util.concurrent.TimeUnit

import akka.actor.typed.{ActorSystem, DispatcherSelector, Extension, ExtensionId}
import com.github.codelionx.distod.ActorSystem.{FOLLOWER, LEADER, Role}
import com.github.codelionx.distod.Settings.{InputParsingSettings, MonitoringSettings, PartitionCompactionSettings, PruningSettings}
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

  trait PartitionCompactionSettings {

    def enabled: Boolean

    def interval: FiniteDuration
  }

  trait MonitoringSettings {

    def interval: FiniteDuration

    def heapEvictionThreshold: Double

    def statisticsLogInterval: FiniteDuration

    def statisticsLogLevel: String
  }

  trait PruningSettings {

    def odSizeLimit: Option[Int]

    def interestingnessThreshold: Option[Long]

    def pruneOdSize: Boolean = odSizeLimit.isDefined

    def pruneInterestingness: Boolean = interestingnessThreshold.isDefined
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

  val maxParallelism: Int = {
    val path = s"$namespace.max-parallelism"
    val value = config.getInt(path)
    if (value < 1)
      throw new ConfigException.BadValue(path, "value can not be 0 (or less)")
    value
  }

  val maxWorkers: Int = config.getInt(s"$namespace.max-workers")

  private val cores = Runtime.getRuntime.availableProcessors()

  val parallelism: Int = Seq(maxParallelism, cores).min

  val numberOfWorkers: Int = Seq(maxWorkers, maxParallelism, cores).min

  val concurrentWorkerJobs: Int = config.getInt(s"$namespace.concurrent-worker-jobs")

  val expectedNodeCount: Int = config.getInt(s"$namespace.expected-node-count")

  val directPartitionProductThreshold: Int = config.getInt(s"$namespace.direct-partition-product-threshold")

  val cpuBoundTaskDispatcher: DispatcherSelector =
    DispatcherSelector.fromConfig(s"$namespace.cpu-bound-tasks-dispatcher")

  val cacheEnabled: Boolean = config.getBoolean(s"$namespace.enable-partition-cache")
  val cacheDisabled: Boolean = !cacheEnabled

  val partitionCompactionSettings: PartitionCompactionSettings = new PartitionCompactionSettings {

    private val subnamespace = s"$namespace.partition-compaction"

    override def enabled: Boolean = cacheEnabled && config.getBoolean(s"$subnamespace.enabled")

    // cuts off nanosecond part of durations (we dont care about this, because duration should be in
    // seconds or greater anyway)
    override def interval: FiniteDuration = FiniteDuration.apply(
      config.getDuration(s"$subnamespace.interval").getSeconds,
      TimeUnit.SECONDS
    )
  }

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

  val monitoringSettings: MonitoringSettings = new MonitoringSettings {

    private val subnamespace = s"$namespace.monitoring"

    override val interval: FiniteDuration = {
      val duration = config.getDuration(s"$subnamespace.interval")
      val finiteDurationOnlySeconds = FiniteDuration(duration.getSeconds, TimeUnit.SECONDS)
      val finiteDurationOnlyNanos = FiniteDuration(duration.getNano, TimeUnit.NANOSECONDS)
      finiteDurationOnlySeconds + finiteDurationOnlyNanos
    }

    override val heapEvictionThreshold: Double = config.getInt(s"$subnamespace.heap-eviction-threshold") match {
      case i if i <= 0 || i > 100 => throw new ConfigException.BadValue(
        s"$subnamespace.heap-eviction-threshold",
        s"threshold must be between [excluding] 0 and [including] 100 (percent value)"
      )
      case i => i / 100.0
    }

    override val statisticsLogInterval: FiniteDuration = {
      val duration = config.getDuration(s"$subnamespace.statistics-log-interval")
      val finiteDurationOnlySeconds = FiniteDuration(duration.getSeconds, TimeUnit.SECONDS)
      val finiteDurationOnlyNanos = FiniteDuration(duration.getNano, TimeUnit.NANOSECONDS)
      finiteDurationOnlySeconds + finiteDurationOnlyNanos
    }

    override val statisticsLogLevel: String = config.getString(s"$subnamespace.statistics-log-level")
  }

  val pruning: PruningSettings = new PruningSettings {

    private val subnamespace = s"$namespace.pruning"

    override val odSizeLimit: Option[Int] =
      if (config.hasPath(s"$subnamespace.od-size-limit"))
        config.getInt(s"$subnamespace.od-size-limit") match {
          case i if i < 1 => throw new ConfigException.BadValue(
            s"$subnamespace.od-size-limit",
            s"limit must be greater than 0"
          )
          case i => Some(i)
        }
      else
        None

    override val interestingnessThreshold: Option[Long] =
      if (config.hasPath(s"$subnamespace.interestingness-threshold"))
        Some(config.getLong(s"$subnamespace.interestingness-threshold"))
      else
        None
  }

  val shutdownTimeout: FiniteDuration = {
    val duration = config.getDuration(s"$namespace.shutdown-timeout")
    val finiteDurationOnlySeconds = FiniteDuration(duration.getSeconds, TimeUnit.SECONDS)
    val finiteDurationOnlyNanos = FiniteDuration(duration.getNano, TimeUnit.NANOSECONDS)
    finiteDurationOnlySeconds + finiteDurationOnlyNanos
  }
}
