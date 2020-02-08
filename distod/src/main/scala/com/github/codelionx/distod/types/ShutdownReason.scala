package com.github.codelionx.distod.types

import akka.actor.CoordinatedShutdown
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import com.github.codelionx.distod.Serialization.CborSerializable


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[ShutdownReason.AlgorithmFinished], name = "AlgorithmFinishedReason"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.AllActorsDied], name = "AllActorsDied"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.JvmExit], name = "JvmExit"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.Unknown], name = "Unknown"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.ClusterDowning], name = "ClusterDowning"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.ClusterLeaving], name = "ClusterLeaving"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.ActorSystemTerminate], name = "ActorSystemTerminate"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.ClusterJoinUnsuccessful], name = "ClusterJoinUnsuccessful"),
  new JsonSubTypes.Type(value = classOf[ShutdownReason.IncompatibleConfigurationDetected], name = "IncompatibleConfigurationDetected"),
))
trait ShutdownReason extends CborSerializable {
  @transient
  def reason: CoordinatedShutdown.Reason
}

object ShutdownReason {

  // custom reasons
  case object AlgorithmFinishedReason extends CoordinatedShutdown.Reason
  case object AllActorsDiedReason extends CoordinatedShutdown.Reason

  // wrappers
  @JsonTypeName("JvmExit")
  case class JvmExit() extends ShutdownReason {
    // make sure that the singleton objects are correctly initialized in the other VMs
    @transient
    override lazy val reason: CoordinatedShutdown.JvmExitReason.type = CoordinatedShutdown.JvmExitReason
  }

  @JsonTypeName("AlgorithmFinishedReason")
  case class AlgorithmFinished() extends ShutdownReason {
    @transient
    override lazy val reason: AlgorithmFinishedReason.type = AlgorithmFinishedReason
  }

  @JsonTypeName("AllActorsDiedReason")
  case class AllActorsDied() extends ShutdownReason {
    @transient
    override lazy val reason: AllActorsDiedReason.type = AllActorsDiedReason
  }

  @JsonTypeName("Unknown")
  case class Unknown() extends ShutdownReason {
    @transient
    override lazy val reason: CoordinatedShutdown.UnknownReason.type = CoordinatedShutdown.UnknownReason
  }

  @JsonTypeName("ClusterDowning")
  case class ClusterDowning() extends ShutdownReason {
    @transient
    override lazy val reason: CoordinatedShutdown.ClusterDowningReason.type = CoordinatedShutdown.ClusterDowningReason
  }

  @JsonTypeName("ClusterLeaving")
  case class ClusterLeaving() extends ShutdownReason {
    @transient
    override lazy val reason: CoordinatedShutdown.ClusterLeavingReason.type = CoordinatedShutdown.ClusterLeavingReason
  }

  @JsonTypeName("ActorSystemTerminate")
  case class ActorSystemTerminate() extends ShutdownReason {
    @transient
    override lazy val reason: CoordinatedShutdown.ActorSystemTerminateReason.type = CoordinatedShutdown.ActorSystemTerminateReason
  }

  @JsonTypeName("ClusterJoinUnsuccessful")
  case class ClusterJoinUnsuccessful() extends ShutdownReason {
    @transient
    override lazy val reason: CoordinatedShutdown.ClusterJoinUnsuccessfulReason.type = CoordinatedShutdown.ClusterJoinUnsuccessfulReason
  }

  @JsonTypeName("IncompatibleConfigurationDetected")
  case class IncompatibleConfigurationDetected() extends ShutdownReason {
    @transient
    override lazy val reason: CoordinatedShutdown.IncompatibleConfigurationDetectedReason.type = CoordinatedShutdown.IncompatibleConfigurationDetectedReason
  }

  def from(reason: Option[CoordinatedShutdown.Reason]): ShutdownReason = reason match {
    case Some(r) => from(r)
    case None => Unknown()
  }

  def from(reason: CoordinatedShutdown.Reason): ShutdownReason = reason match {
    case CoordinatedShutdown.JvmExitReason => JvmExit()
    case CoordinatedShutdown.ClusterDowningReason => ClusterDowning()
    case CoordinatedShutdown.ClusterLeavingReason => ClusterLeaving()
    case CoordinatedShutdown.ActorSystemTerminateReason => ActorSystemTerminate()
    case CoordinatedShutdown.ClusterJoinUnsuccessfulReason => ClusterJoinUnsuccessful()
    case CoordinatedShutdown.IncompatibleConfigurationDetectedReason => IncompatibleConfigurationDetected()
    case AlgorithmFinishedReason => AlgorithmFinished()
    case AllActorsDiedReason => AllActorsDied()
    case CoordinatedShutdown.UnknownReason | _ => Unknown()
  }

}