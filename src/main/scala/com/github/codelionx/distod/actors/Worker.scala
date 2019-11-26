package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.Master.{CandidateNodeChecked, DispatchWork}
import com.github.codelionx.distod.actors.Worker.{CheckCandidateNode, Command, WrappedPartitionEvent}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{FoundDependencies, ResultProxyCommand}
import com.github.codelionx.distod.types.{CandidateSet, PendingJobMap}
import com.github.codelionx.distod.types.OrderDependency.ConstantOrderDependency


object Worker {

  sealed trait Command extends CborSerializable
  final case class CheckCandidateNode(
      candidateId: CandidateSet,
      spiltCandidates: CandidateSet,
      swapCandidates: Seq[(Int, Int)]
  ) extends Command
  private case class WrappedPartitionEvent(event: PartitionEvent) extends Command

  def name(n: Int): String = s"worker-$n"

  def apply(
      partitionManager: ActorRef[PartitionCommand], rsProxy: ActorRef[ResultProxyCommand],
      master: ActorRef[Master.Command]
  ): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      new Worker(context, master, partitionManager, rsProxy).start()
    }

}


class Worker(
    context: ActorContext[Command],
    master: ActorRef[Master.Command],
    partitionManager: ActorRef[PartitionCommand],
    rsProxy: ActorRef[ResultProxyCommand]
) {

  private val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))

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
    case task @ CheckCandidateNode(candidateId, spiltCandidates, swapCandidates) =>
      context.log.info("Checking candidate node {}", candidateId)

      checkSplitCandidates(attributes, task)

    case WrappedPartitionEvent(event) =>
      context.log.info("Ignored {}", event)
      Behaviors.same
  }

  def checkSplitCandidates(attributes: Seq[Int], task: CheckCandidateNode): Behavior[Command] = {
    partitionManager ! LookupError(task.candidateId, partitionEventMapper)

    for (c <- task.spiltCandidates) {
      val fdContext = task.candidateId - c
      partitionManager ! LookupError(fdContext, partitionEventMapper)
    }

    context.log.debug("Loading partition errors for all split checks")

    def collectErrors(errors: PendingJobMap[CandidateSet, Double], expected: Int): Behavior[Command] =
      Behaviors.receiveMessagePartial {
        case WrappedPartitionEvent(ErrorFound(key, value)) =>
          context.log.debug("Received partition error value", key, value)
          val newErrorMap = errors + (key -> value)
          if (newErrorMap.size == expected) {
            performCheck(newErrorMap)
          } else {
            collectErrors(errors + (key -> value), expected)
          }
      }

    def performCheck(errors: PendingJobMap[CandidateSet, Double]): Behavior[Command] = {
      val errorCompare = errors(task.candidateId)
      val validConstantODs = for {
        a <- task.spiltCandidates.unsorted
        fdContext = task.candidateId - a
        if errors(fdContext) == errorCompare // candidate fdContext: [] -> a holds
      } yield fdContext -> a

      if (validConstantODs.nonEmpty) {
        val constantOds = validConstantODs.map {
          case (context, attribute) => ConstantOrderDependency(context, attribute)
        }.toSeq
        context.log.debug("Found valid candidates: {}", constantOds.mkString(", "))
        rsProxy ! FoundDependencies(constantOds)
      } else {
        context.log.debug("No valid constant candidates found")
      }

      val removedCandidates = {
        val validCandidateSet = CandidateSet.fromSpecific(validConstantODs.map(_._2))
        if (validCandidateSet.nonEmpty)
          validCandidateSet union (CandidateSet.fromSpecific(attributes) diff task.candidateId)
        else
          CandidateSet.empty
      }

      checkSwapCandidates(attributes, task, removedCandidates)
    }

    collectErrors(PendingJobMap.empty, task.spiltCandidates.size + 1)
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