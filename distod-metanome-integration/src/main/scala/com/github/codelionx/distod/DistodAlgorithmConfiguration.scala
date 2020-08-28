package com.github.codelionx.distod

import java.{lang, util}

import com.github.codelionx.distod.DistodAlgorithm.inputGeneratorIdentifier
import com.typesafe.config.{Config, ConfigFactory}
import de.metanome.algorithm_integration.AlgorithmConfigurationException
import de.metanome.algorithm_integration.algorithm_types.{BooleanParameterAlgorithm, IntegerParameterAlgorithm, StringParameterAlgorithm}
import de.metanome.algorithm_integration.configuration.{ConfigurationRequirement, ConfigurationRequirementInteger, ConfigurationRequirementRelationalInput}

import scala.jdk.CollectionConverters._


trait DistodAlgorithmConfiguration
  extends StringParameterAlgorithm
    with IntegerParameterAlgorithm
    with BooleanParameterAlgorithm {

  private val settings: util.Map[String, Any] = new util.HashMap()

  protected val configurationRequirements: Seq[ConfigurationRequirement[_]] =
    DistodParameters.all.map(_.configurationRequirement)

  protected def parseConfig: Config = ConfigFactory.parseMap(settings)

  override def setStringConfigurationValue(identifier: String, values: String*): Unit = {
    if (identifier != null)
      if (DistodParameters.all.exists(_.identifier == identifier))
        settings.put(identifier, values(0))
      else
        handleUnknownConfigurationValue(identifier, values)
  }

  override def setIntegerConfigurationValue(identifier: String, values: Integer*): Unit = {
    if (identifier != null)
      if (DistodParameters.all.exists(_.identifier == identifier)) {
        if (!(identifier == DistodParameters.PruningOdSizeLimit.identifier &&
          values(0) == DistodParameters.PruningOdSizeLimit.defaultValue) &&
          !(identifier == DistodParameters.PruningInterestingnessThreshold.identifier &&
            values(0) == DistodParameters.PruningInterestingnessThreshold.defaultValue))
          settings.put(identifier, values(0))
      } else
        handleUnknownConfigurationValue(identifier, values)
  }

  override def setBooleanConfigurationValue(identifier: String, values: lang.Boolean*): Unit = {
    if (identifier != null)
      if (DistodParameters.all.exists(_.identifier == identifier))
        settings.put(identifier, values(0))
      else
        handleUnknownConfigurationValue(identifier, values)
  }

  override def getConfigurationRequirements: util.ArrayList[ConfigurationRequirement[_]] = {
    val configurationRequirement: ConfigurationRequirementInteger = {
      val req = new ConfigurationRequirementInteger("test")
      req.setDefaultValues(Array[java.lang.Integer](0))
      req.setRequired(true)
      req
    }
    val reqs = Seq(
      new ConfigurationRequirementRelationalInput(inputGeneratorIdentifier)
    ) :+ configurationRequirement
    new util.ArrayList(reqs.asJava)
  }

  protected def handleUnknownConfigurationValue(identifier: String, values: Seq[Any]): Unit =
    throw new AlgorithmConfigurationException(
      s"Unknown configuration for parameter $identifier! Supplied values: ${values.mkString(", ")}"
    )
}
