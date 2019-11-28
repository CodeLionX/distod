package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.Master
import com.github.codelionx.distod.actors.Master.{CandidateNodeChecked, DispatchWork, JobType}
import com.github.codelionx.distod.actors.Master.JobType.JobType
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


class Worker(workerContext: WorkerContext) {

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

      val splitValidation = SplitCandidateValidationBehavior(workerContext, attributes, candidateId, splitCandidates) _

      if (splitCandidates.nonEmpty) {
        splitValidation(removedSplitCandidates =>
          handleResults(attributes, candidateId, JobType.Split, removedSplitCandidates = removedSplitCandidates)
        )
      } else {
        handleResults(attributes, candidateId, JobType.Split)
      }

    case CheckSwapCandidates(candidateId, swapCandidates) =>
      context.log.info("Checking swap candidates of node {}", candidateId)

      val swapValidation = SwapCandidateValidationBehavior(workerContext, candidateId, swapCandidates) _

      if (swapCandidates.nonEmpty) {
        swapValidation(removedSwapCandidates =>
          handleResults(attributes, candidateId, JobType.Swap, removedSwapCandidates = removedSwapCandidates)
        )
      } else {
        handleResults(attributes, candidateId, JobType.Swap)
      }

    case WrappedPartitionEvent(event) =>
      context.log.info("Ignored {}", event)
      Behaviors.same
  }

  def handleResults(
      attributes: Seq[Int],
      candidateId: CandidateSet,
      jobType: JobType,
      removedSplitCandidates: CandidateSet = CandidateSet.empty,
      removedSwapCandidates: Seq[(Int, Int)] = Seq.empty
  ): Behavior[Command] = {

    // notify master of result
    master ! CandidateNodeChecked(candidateId, jobType, removedSplitCandidates, removedSwapCandidates)

    // ready to work on next node:
    master ! DispatchWork(context.self)
    behavior(attributes)
  }
}