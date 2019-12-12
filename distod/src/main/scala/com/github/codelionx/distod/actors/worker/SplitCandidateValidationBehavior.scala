package com.github.codelionx.distod.actors.worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.worker.Worker.{Command, WrappedPartitionEvent}
import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.{ErrorFound, LookupError}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.FoundDependencies
import com.github.codelionx.distod.types.CandidateSet


object SplitCandidateValidationBehavior {

  def apply(
      workerContext: WorkerContext,
      attributes: Seq[Int],
      candidateId: CandidateSet,
      splitCandidates: CandidateSet
  )(
      next: CandidateSet => Behavior[Command]
  ): Behavior[Command] =
    new SplitCandidateValidationBehavior(workerContext, next, attributes, candidateId, splitCandidates).start()
}


class SplitCandidateValidationBehavior(
    workerContext: WorkerContext,
    next: CandidateSet => Behavior[Command],
    attributes: Seq[Int],
    candidateId: CandidateSet,
    splitCandidates: CandidateSet
) extends CandidateValidation {

  import workerContext._


  def start(): Behavior[Command] = {
    context.log.debug("Loading partition errors for all split checks")
    partitionManager ! LookupError(candidateId, partitionEventMapper)

    for (c <- splitCandidates) {
      val fdContext = candidateId - c
      partitionManager ! LookupError(fdContext, partitionEventMapper)
    }

    collectErrors(Map.empty, splitCandidates.size + 1)
  }

  def collectErrors(errors: Map[CandidateSet, Double], expected: Int): Behavior[Command] =
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

  def performCheck(errors: Map[CandidateSet, Double]): Behavior[Command] = {
    val result = checkSplitCandidates(candidateId, splitCandidates, attributes, errors)

    if (result.validOds.nonEmpty) {
      context.log.debug("Found valid candidates: {}", result.validOds.mkString(", "))
      rsProxy ! FoundDependencies(result.validOds)
    } else {
      context.log.debug("No valid constant candidates found")
    }

    next(result.removedCandidates)
  }
}
