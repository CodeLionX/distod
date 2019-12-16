package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.github.codelionx.distod.Serialization.{CandidateSetKeyDeserializer, CandidateSetKeySerializer, CborSerializable}
import com.github.codelionx.distod.actors.master.{CandidateState, Master, WorkQueue}
import com.github.codelionx.distod.actors.master.Master.{DispatchWork, NewCandidates, SplitCandidatesChecked, SwapCandidatesChecked}
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand
import com.github.codelionx.distod.types.CandidateSet


object Worker {

  sealed trait Command extends CborSerializable
  final case class CheckSplitCandidates(
      candidateId: CandidateSet,
      splitCandidates: CandidateSet
  ) extends Command
  final case class CheckSwapCandidates(
      candidateId: CandidateSet,
      swapCandidates: Seq[(Int, Int)]
  ) extends Command
  final case class GenerateCandidates(
      candidateId: CandidateSet,
      @JsonSerialize(keyUsing = classOf[CandidateSetKeySerializer])
      @JsonDeserialize(keyUsing = classOf[CandidateSetKeyDeserializer])
      state: Map[CandidateSet, CandidateState],
      workQueue: WorkQueue
  ) extends Command
  private[worker] case class WrappedPartitionEvent(event: PartitionEvent) extends Command

  def name(n: Int): String = s"worker-$n"

  def apply(
      partitionManager: ActorRef[PartitionCommand],
      rsProxy: ActorRef[ResultProxyCommand],
      master: ActorRef[Master.Command]
  ): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))
      new Worker(WorkerContext(context, master, partitionManager, rsProxy, partitionEventMapper)).start()
    }

}


class Worker(workerContext: WorkerContext) extends CandidateGeneration {

  import workerContext._
  import Worker._


  def start(): Behavior[Command] = initialize()

  def initialize(): Behavior[Command] = {
    partitionManager ! LookupAttributes(partitionEventMapper)

    Behaviors.receiveMessagePartial {
      case WrappedPartitionEvent(AttributesFound(attributes)) =>
        context.log.debug("Worker ready to process candidates: Master={}, Attributes={}", master, attributes)
        master ! DispatchWork(context.self)
        behavior(attributes)
    }
  }

  def behavior(attributes: Seq[Int]): Behavior[Command] = Behaviors.receiveMessage {
    case CheckSplitCandidates(candidateId, splitCandidates) =>
      context.log.info("Checking split candidates of node {}", candidateId)

      def handleResults(removedSplitCandidates: CandidateSet = CandidateSet.empty): Behavior[Command] = {
        // notify master of result
        master ! SplitCandidatesChecked(candidateId, removedSplitCandidates)

        // ready to work on next node:
        master ! DispatchWork(context.self)
        behavior(attributes)
      }

      if (splitCandidates.nonEmpty) {
        SplitCandidateValidationBehavior(workerContext, attributes, candidateId, splitCandidates)(
          removedSplitCandidates => handleResults(removedSplitCandidates)
        )
      } else {
        handleResults()
      }

    case CheckSwapCandidates(candidateId, swapCandidates) =>
      context.log.info("Checking swap candidates of node {}", candidateId)

      def handleResults(removedSwapCandidates: Seq[(Int, Int)] = Seq.empty): Behavior[Command] = {
        // notify master of result
        master ! SwapCandidatesChecked(candidateId, removedSwapCandidates)

        // ready to work on next node:
        master ! DispatchWork(context.self)
        behavior(attributes)
      }

      if (swapCandidates.nonEmpty) {
        SwapCandidateValidationBehavior(workerContext, candidateId, swapCandidates)(
          removedSwapCandidates => handleResults(removedSwapCandidates)
        )
      } else {
        handleResults()
      }

    case GenerateCandidates(candidateId, state, workQueue) =>
      context.log.info("Generating new candidates from node {}", candidateId)
      val (jobs, stateUpdates) = generateNewCandidates(attributes, state, workQueue, candidateId)
      master ! NewCandidates(candidateId, jobs, stateUpdates)
      master ! DispatchWork(context.self)
      Behaviors.same

    case WrappedPartitionEvent(event) =>
      context.log.info("Ignored {}", event)
      Behaviors.same
  }
}