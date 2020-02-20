package com.github.codelionx.distod.actors.worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.worker.Worker.{Command, WrappedPartitionEvent}
import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.{ErrorFound, LookupError}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.FoundDependencies
import com.github.codelionx.distod.types.CandidateSet
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
  private val job = new CheckSplitJob(candidateId, splitCandidates)

  def start(): Behavior[Command] = {
    context.log.trace("Loading partition errors for split checks")
    for (c <- job.errorIds) {
      partitionManager ! LookupError(candidateId, c, partitionEventMapper)
    }
    behavior()
  }

  private def behavior(): Behavior[Command] = Behaviors.receiveMessage {
    case WrappedPartitionEvent(ErrorFound(_, key, value)) =>
      context.log.trace("Received partition error value: {}, {}", key, value)
      job.receivedError(key, value)

      spans.begin("Split check")
      val finished = job.performPossibleChecks()
      spans.end("Split check")

      if (finished) {
        val (validODs, removedCandidates) = job.results(attributes)
        if (validODs.nonEmpty) {
          context.log.trace("Found valid candidates: {}", validODs.mkString(", "))
          rsProxy ! FoundDependencies(validODs)
        } else {
          context.log.trace("No valid constant candidates found")
        }
        next(removedCandidates)
      } else {
        Behaviors.same
      }

    case m =>
      stash.stash(m)
      Behaviors.same
  }
}
