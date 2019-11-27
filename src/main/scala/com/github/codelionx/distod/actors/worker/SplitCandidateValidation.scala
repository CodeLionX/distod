package com.github.codelionx.distod.actors.worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.worker.Worker.{Command, WrappedPartitionEvent}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.{ErrorFound, LookupError}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.FoundDependencies
import com.github.codelionx.distod.types.{CandidateSet, PendingJobMap}
import com.github.codelionx.distod.types.OrderDependency.ConstantOrderDependency


object SplitCandidateValidation {

  def apply(
      workerContext: WorkerContext,
      attributes: Seq[Int],
      candidateId: CandidateSet,
      splitCandidates: CandidateSet
  )(
      next: CandidateSet => Behavior[Command]
  ): Behavior[Command] =
    new SplitCandidateValidation(workerContext, next, attributes, candidateId, splitCandidates).start()
}


class SplitCandidateValidation(
    workerContext: WorkerContext,
    next: CandidateSet => Behavior[Command],
    attributes: Seq[Int],
    candidateId: CandidateSet,
    splitCandidates: CandidateSet
) {

  import workerContext._


  def start(): Behavior[Command] = {
    context.log.debug("Loading partition errors for all split checks")
    partitionManager ! LookupError(candidateId, partitionEventMapper)

    for (c <- splitCandidates) {
      val fdContext = candidateId - c
      partitionManager ! LookupError(fdContext, partitionEventMapper)
    }

    collectErrors(PendingJobMap.empty, splitCandidates.size + 1)
  }

  def collectErrors(errors: PendingJobMap[CandidateSet, Double], expected: Int): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case WrappedPartitionEvent(ErrorFound(key, value)) =>
        context.log.debug("Received partition error value: {}, {}", key, value)
        val newErrorMap = errors + (key -> value)
        if (newErrorMap.size == expected) {
          performCheck(newErrorMap)
        } else {
          collectErrors(newErrorMap, expected)
        }
    }

  def performCheck(errors: PendingJobMap[CandidateSet, Double]): Behavior[Command] = {
    val errorCompare = errors(candidateId)
    val validConstantODs = for {
      a <- splitCandidates.unsorted
      fdContext = candidateId - a
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
        validCandidateSet union (CandidateSet.fromSpecific(attributes) diff candidateId)
      else
        CandidateSet.empty
    }

    next(removedCandidates)
  }
}
