package com.github.codelionx.distod.actors.worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.worker.Worker.{Command, WrappedPartitionEvent}
import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.{ErrorFound, LookupError}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.FoundDependencies
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.ConstantOrderDependency
import com.github.codelionx.util.timing.Timing


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


  private val timing = Timing(context.system)
  private val spans = timing.createSpans

  def start(): Behavior[Command] = {
    context.log.trace("Loading partition errors for split checks")
    partitionManager ! LookupError(candidateId, partitionEventMapper)

    for (c <- splitCandidates) {
      val fdContext = candidateId - c
      partitionManager ! LookupError(fdContext, partitionEventMapper)
    }

    collectErrors(Map.empty, splitCandidates)
  }

  private def collectErrors(errors: Map[CandidateSet, Double], toBeChecked: CandidateSet): Behavior[Command] =
    Behaviors.receiveMessage {
      case WrappedPartitionEvent(ErrorFound(`candidateId`, value)) =>
        context.log.trace("Received partition error value: {}, {}", candidateId, value)
        changeToChecking(value, errors, toBeChecked)

      case WrappedPartitionEvent(ErrorFound(key, value)) =>
        context.log.trace("Received partition error value: {}, {}", key, value)
        val newErrorMap = errors + (key -> value)
        collectErrors(newErrorMap, toBeChecked)

      case m =>
        stash.stash(m)
        Behaviors.same
    }

  private def checking(errorCompare: Double, toBeChecked: CandidateSet, validCandidates: Set[Int]): Behavior[Command] =
    Behaviors.receiveMessage {
      case WrappedPartitionEvent(ErrorFound(`candidateId`, _)) =>
        // ignore, should not happen
        context.log.error("Received unexpected")
        Behaviors.same

      case WrappedPartitionEvent(ErrorFound(key, value)) =>
        context.log.trace("Received partition error value: {}, {}", key, value)
        spans.begin("Split check")
        val attributeSet = candidateId diff key
        attributeSet.headOption match {
          case Some(attribute) if toBeChecked.contains(attribute) =>
            val isValid = checkSplitCandidate(value, errorCompare)
            val newValidCandidates =
              if (isValid) validCandidates + attribute
              else validCandidates
            val remainingCandidates = toBeChecked - attribute

            spans.end("Split check")
            if (remainingCandidates.isEmpty)
              processResults(newValidCandidates)
            else
              checking(errorCompare, toBeChecked - attribute, newValidCandidates)

          case None =>
            context.log.warn("Received unnecessary partition with key {}", key)
            spans.end("Split check")
            checking(errorCompare, toBeChecked, validCandidates)
        }

      case m =>
        stash.stash(m)
        Behaviors.same
    }

  private def changeToChecking(
      errorCompare: Double, errors: Map[CandidateSet, Double], toBeChecked: CandidateSet
  ): Behavior[Command] = {
    spans.begin("Split check")
    val candidates = for {
      a <- toBeChecked.unsorted
      context = candidateId - a
      errorContext <- errors.get(context)
    } yield a -> checkSplitCandidate(errorContext, errorCompare)

    val validCandidates = candidates.filter(t => t._2).map(t => t._1)
    val remainingCandidates = toBeChecked diff candidates.map(t => t._1)

    spans.end("Split check")
    if (remainingCandidates.isEmpty)
      processResults(validCandidates)
    else
      checking(errorCompare, remainingCandidates, validCandidates)
  }

  private def processResults(validCandidates: Set[Int]): Behavior[Command] = {
    if (validCandidates.nonEmpty) {
      val validODs = validCandidates.map(a => ConstantOrderDependency(candidateId - a, a)).toSeq
      context.log.trace("Found valid candidates: {}", validODs.mkString(", "))
      rsProxy ! FoundDependencies(validODs)
    } else {
      context.log.trace("No valid constant candidates found")
    }

    val removedCandidates = {
      val validCandidateSet = CandidateSet.fromSpecific(validCandidates)
      if (validCandidateSet.nonEmpty)
        validCandidateSet union (CandidateSet.fromSpecific(attributes) diff candidateId)
      else
        CandidateSet.empty
    }
    next(removedCandidates)
  }
}
