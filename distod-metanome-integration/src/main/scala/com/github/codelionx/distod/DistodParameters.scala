package com.github.codelionx.distod

import de.metanome.algorithm_integration.configuration._


object DistodParameters {

  sealed trait Parameter[T <: Any] {
    def identifier: String

    def defaultValue: T

    def configurationRequirement: ConfigurationRequirementDefaultValue[T, _ <: ConfigurationSetting]
  }

  sealed trait StringParameter extends Parameter[String] {
    def configurationRequirement: ConfigurationRequirementString = {
      val req = new ConfigurationRequirementString(identifier)
      req.setDefaultValues(Array(defaultValue))
      req.setRequired(false)
      req
    }
  }

  sealed trait IntParameter extends Parameter[java.lang.Integer] {
    def configurationRequirement: ConfigurationRequirementInteger = {
      val req = new ConfigurationRequirementInteger(identifier)
      req.setDefaultValues(Array(defaultValue))
      req.setRequired(false)
      req
    }
  }

  sealed trait BooleanParameter extends Parameter[java.lang.Boolean] {
    def configurationRequirement: ConfigurationRequirementBoolean = {
      val req = new ConfigurationRequirementBoolean(identifier)
      req.setDefaultValues(Array(defaultValue))
      req.setRequired(false)
      req
    }
  }

  case object Host extends StringParameter {
    val identifier = "host"
    val defaultValue = "127.0.0.1"
  }

  case object Port extends IntParameter {
    val identifier = "port"
    val defaultValue = 7878
  }

  case object MaxParallelism extends IntParameter {
    val identifier = "max-parallelism"
    val defaultValue = 512
  }

  case object ConcurrentWorkerJobs extends IntParameter {
    val identifier = "concurrent-worker-jobs"
    val defaultValue = 4
  }

  case object ExpectedNodeCount extends IntParameter {
    val identifier = "expected-node-count"
    val defaultValue = 20
  }

  case object DirectPartitionProductThreshold extends IntParameter {
    val identifier = "direct-partition-product-threshold"
    val defaultValue = 15
  }

  case object EnablePartitionCache extends BooleanParameter {
    val identifier = "enable-partition-cache"
    val defaultValue = true
  }

  case object PartitionCompactionEnabled extends BooleanParameter {
    val identifier = "partition-compaction.enabled"
    val defaultValue = true
  }

  case object PartitionCompactionInterval extends StringParameter {
    val identifier = "partition-compaction.interval"
    val defaultValue = "40s"
  }

  case object MonitoringInterval extends StringParameter {
    val identifier = "monitoring.interval"
    val defaultValue = "1s"
  }

  case object MonitoringHeapEvictionThreshold extends IntParameter {
    val identifier = "monitoring.heap-eviction-threshold"
    val defaultValue = 90
  }

  case object PruningOdSizeLimit extends IntParameter {
    val identifier = "pruning.od-size-limit"
    val defaultValue = 0 // 5
  }

  case object PruningInterestingnessThreshold extends IntParameter {
    val identifier = "pruning.interestingness-threshold"
    val defaultValue = 0 // 10000000
  }

  val all = Seq(
    Host, Port, MaxParallelism, ConcurrentWorkerJobs, ExpectedNodeCount, DirectPartitionProductThreshold,
    EnablePartitionCache, PartitionCompactionEnabled, PartitionCompactionInterval, MonitoringInterval,
    MonitoringHeapEvictionThreshold, PruningOdSizeLimit, PruningInterestingnessThreshold
  )

}
