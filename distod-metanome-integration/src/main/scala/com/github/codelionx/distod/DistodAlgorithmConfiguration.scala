package com.github.codelionx.distod

import java.{lang, util}

import com.typesafe.config.{Config, ConfigFactory}
import de.metanome.algorithm_integration.AlgorithmConfigurationException
import de.metanome.algorithm_integration.algorithm_types.{BooleanParameterAlgorithm, IntegerParameterAlgorithm, StringParameterAlgorithm}
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement


trait DistodAlgorithmConfiguration
  extends StringParameterAlgorithm
    with IntegerParameterAlgorithm
    with BooleanParameterAlgorithm {

  private val settings: util.Map[String, Any] = new util.HashMap()

  protected val configurationRequirements: Seq[ConfigurationRequirement[_]] =
    DistodParameters.all.map(_.configurationRequirement)

  protected def parseConfig: Config = ConfigFactory.parseMap(settings)

  override def setStringConfigurationValue(identifier: String, values: String*): Unit =
    checked(identifier, values) {
      settings.put(identifier, values(0))
    }

  override def setIntegerConfigurationValue(identifier: String, values: Integer*): Unit =
    checked(identifier, values) {
      if (!(identifier == DistodParameters.PruningOdSizeLimit.identifier &&
        values(0) == DistodParameters.PruningOdSizeLimit.defaultValue) &&
        !(identifier == DistodParameters.PruningInterestingnessThreshold.identifier &&
          values(0) == DistodParameters.PruningInterestingnessThreshold.defaultValue))
        settings.put(identifier, values(0))
    }

  override def setBooleanConfigurationValue(identifier: String, values: lang.Boolean*): Unit =
    checked(identifier, values) {
      settings.put(identifier, values(0))
    }

  private def checked(identifier: String, values: Seq[Any])(f: => Unit): Unit =
    if (identifier != null)
      if (DistodParameters.all.exists(_.identifier == identifier))
        f
      else
        throw new AlgorithmConfigurationException(
          s"Unknown configuration for parameter $identifier! Supplied values: ${values.mkString(", ")}"
        )

}
