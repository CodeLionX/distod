package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.Master
import com.github.codelionx.distod.actors.master.Master.{DispatchWork, SplitCandidatesChecked, SwapCandidatesChecked}
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.timing.Timing


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

  import Worker._
  import workerContext._

  private val timing = Timing(context.system).spans

  def start(): Behavior[Command] = initialize()

  def initialize(): Behavior[Command] = {
    partitionManager ! LookupAttributes(partitionEventMapper)

    Behaviors.receiveMessagePartial {
      case WrappedPartitionEvent(AttributesFound(attributes)) =>
        context.log.trace("Worker ready to process candidates: Master={}, Attributes={}", master, attributes)
        master ! DispatchWork(context.self)
        behavior(attributes)
    }
  }

  def behavior(attributes: Seq[Int]): Behavior[Command] = Behaviors.receiveMessage {
    case CheckSplitCandidates(candidateId, splitCandidates) =>
      timing.start("Split checks")
      context.log.debug("Checking split candidates of node {}", candidateId)

      def handleResults(removedSplitCandidates: CandidateSet = CandidateSet.empty): Behavior[Command] = {
        // notify master of result
        context.log.trace("Sending results of ({}, Split) to master at {}", candidateId, master)
        master ! SplitCandidatesChecked(candidateId, removedSplitCandidates)

        // ready to work on next node:
        master ! DispatchWork(context.self)
        timing.end("Split checks")
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
      timing.start("Swap checks")
      context.log.debug("Checking swap candidates of node {}", candidateId)

      def handleResults(removedSwapCandidates: Seq[(Int, Int)] = Seq.empty): Behavior[Command] = {
        // notify master of result
        context.log.trace("Sending results of ({}, Swap) to master at {}", candidateId, master)
        master ! SwapCandidatesChecked(candidateId, removedSwapCandidates)

        // ready to work on next node:
        master ! DispatchWork(context.self)
        timing.end("Swap checks")
        behavior(attributes)
      }

      if (swapCandidates.nonEmpty) {
        SwapCandidateValidationBehavior(workerContext, candidateId, swapCandidates)(
          removedSwapCandidates => handleResults(removedSwapCandidates)
        )
      } else {
        handleResults()
      }

    case WrappedPartitionEvent(event) =>
      context.log.debug("Ignored {}", event)
      Behaviors.same
  }
}