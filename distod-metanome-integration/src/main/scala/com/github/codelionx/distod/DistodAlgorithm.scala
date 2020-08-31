package com.github.codelionx.distod

import java.util

import com.github.codelionx.distod.actors.LeaderGuardian
import com.typesafe.config.ConfigFactory
import de.metanome.algorithm_integration.AlgorithmConfigurationException
import de.metanome.algorithm_integration.algorithm_types.{FunctionalDependencyAlgorithm, RelationalInputParameterAlgorithm}
import de.metanome.algorithm_integration.configuration.{ConfigurationRequirement, ConfigurationRequirementRelationalInput}
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver

import scala.jdk.CollectionConverters._


object DistodAlgorithm {
  final val inputGeneratorIdentifier = "INPUT_GENERATOR"
}


class DistodAlgorithm
  extends FunctionalDependencyAlgorithm
    with RelationalInputParameterAlgorithm
    with DistodAlgorithmConfiguration {

  import DistodAlgorithm._


  private var inputGenerator: Option[RelationalInputGenerator] = None
  private var resultReceiver: Option[FunctionalDependencyResultReceiver] = None

  override def getAuthors: String = "Sebastian Schmidl"

  override def getDescription: String = "Discovers set-based bidirectional order dependencies"

  override def setRelationalInputConfigurationValue(identifier: String, values: RelationalInputGenerator*): Unit = {
    if (identifier != inputGeneratorIdentifier)
      throw new AlgorithmConfigurationException(
        s"Unknown relational input generator $identifier! $inputGeneratorIdentifier expected"
      )
    inputGenerator = values.headOption
  }

  override def getConfigurationRequirements: util.ArrayList[ConfigurationRequirement[_]] = {
    val reqs = Seq(new ConfigurationRequirementRelationalInput(inputGeneratorIdentifier)) ++ configurationRequirements
    new util.ArrayList(reqs.asJava)
  }

  override def execute(): Unit = {
    val config = ConfigFactory.load()
    println(s"Config: $config")
    ActorSystem.create(config.getString("distod.system-name"), config, LeaderGuardian())
    println("Test completed")
  }

  override def setResultReceiver(rr: FunctionalDependencyResultReceiver): Unit = {
    resultReceiver = Some(rr)
  }
}
