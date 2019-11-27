package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.Master
import com.github.codelionx.distod.actors.Master.{CandidateNodeChecked, DispatchWork}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand
import com.github.codelionx.distod.types.CandidateSet


object Worker {

  sealed trait Command extends CborSerializable
  final case class CheckCandidateNode(
      candidateId: CandidateSet,
      spiltCandidates: CandidateSet,
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
    case task @ CheckCandidateNode(candidateId, _, _) =>
      context.log.info("Checking candidate node {}", candidateId)

      SplitCandidateValidation(workerContext, attributes, task.candidateId, task.spiltCandidates) { removedCandidates =>
        checkSwapCandidates(attributes, task, removedCandidates)
      }

    case WrappedPartitionEvent(event) =>
      context.log.info("Ignored {}", event)
      Behaviors.same
  }

  def checkSwapCandidates(
      attributes: Seq[Int], task: CheckCandidateNode, removedSplitCandidates: CandidateSet
  ): Behavior[Command] = {
    // TODO: check swap candidates

    // notify master of result
    master ! CandidateNodeChecked(task.candidateId, removedSplitCandidates, Seq.empty)

    // ready to work on next node:
    master ! DispatchWork(context.self)
    behavior(attributes)
  }
}