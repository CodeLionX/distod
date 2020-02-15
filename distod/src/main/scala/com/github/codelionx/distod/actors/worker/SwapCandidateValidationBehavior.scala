package com.github.codelionx.distod.actors.worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.worker.Worker.{Command, WrappedPartitionEvent}
import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.FoundDependencies
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.EquivalencyOrderDependency
import com.github.codelionx.util.timing.Timing


object SwapCandidateValidationBehavior {

  def apply(
      workerContext: WorkerContext,
      candidateId: CandidateSet,
      swapCandidates: Seq[(Int, Int)]
  )(
      next: Seq[(Int, Int)] => Behavior[Command]
  ): Behavior[Command] =
    new SwapCandidateValidationBehavior(workerContext, next, candidateId, swapCandidates).start()
}


class SwapCandidateValidationBehavior(
    workerContext: WorkerContext,
    next: Seq[(Int, Int)] => Behavior[Command],
    candidateId: CandidateSet,
    swapCandidates: Seq[(Int, Int)]
) extends CandidateValidation {

  import workerContext._


  private val timing = Timing(context.system)

  def start(): Behavior[Command] = {
    context.log.trace("Loading partitions for swap checks")

    val distinctAttributes = swapCandidates.flatMap { case (attr1, attr2) => Seq(attr1, attr2) }.distinct
    val singletonPartitionKeys = distinctAttributes.map(attribute => CandidateSet.from(attribute))
    singletonPartitionKeys.foreach(attribute =>
      partitionManager ! LookupPartition(attribute, partitionEventMapper)
    )
    val candidates = swapCandidates
      .map{ case (a, b) =>
        val candidateContext = candidateId - a - b
        partitionManager ! LookupStrippedPartition(candidateContext, partitionEventMapper)
        candidateContext -> (a, b)
      }
      .toMap
    collectPartitions(Map.empty, Map.empty, singletonPartitionKeys.size, candidates)
  }

  private def collectPartitions(
      singletonPartitions: Map[CandidateSet, FullPartition],
      candidatePartitions: Map[CandidateSet, StrippedPartition],
      expectedSingletons: Int,
      checks: Map[CandidateSet, (Int, Int)]
  ): Behavior[Command] = Behaviors.receiveMessage {
    case WrappedPartitionEvent(PartitionFound(key, value)) =>
      context.log.trace("Received full partition {}", key)
      val newSingletonPartitions = singletonPartitions + (key -> value)
      if(newSingletonPartitions.size == expectedSingletons) {
        checkAndNext(newSingletonPartitions, candidatePartitions, checks)
      } else {
        collectPartitions(newSingletonPartitions, candidatePartitions, expectedSingletons, checks)
      }

    case WrappedPartitionEvent(StrippedPartitionFound(key, value)) =>
      context.log.trace("Received stripped partition {}", key)
      val newCandidatePartitions = candidatePartitions + (key -> value)
      collectPartitions(singletonPartitions, newCandidatePartitions, expectedSingletons, checks)

    case m =>
      stash.stash(m)
      Behaviors.same
  }

  private def performChecks(
      singletonPartitions: Map[CandidateSet, FullPartition],
      checks: Map[CandidateSet, (Int, Int)],
      validODs: Seq[EquivalencyOrderDependency]
  ): Behavior[Command] = Behaviors.receiveMessage {
    case WrappedPartitionEvent(PartitionFound(key, _)) =>
      context.log.error("Received duplicated full partition for key {}", key)
      // ignore
      Behaviors.same

    case WrappedPartitionEvent(StrippedPartitionFound(candidateContext, contextPartition)) =>
      val candidatePartitions = Map(candidateContext -> contextPartition)
      checkAndNext(singletonPartitions, candidatePartitions, checks)

    case m =>
      stash.stash(m)
      Behaviors.same
  }

  private def checkAndNext(
      singletonPartitions: Map[CandidateSet, FullPartition],
      candidatePartitions: Map[CandidateSet, StrippedPartition],
      checks: Map[CandidateSet, (Int, Int)],
  ): Behavior[Command] = {
    timing.unsafeTime("Swap check") {
      val validODs = candidatePartitions.flatMap{ case (candidateContext, contextPartition) =>
        val (left, right) = checks(candidateContext)
        val leftPartition = singletonPartitions(CandidateSet.from(left))
        val rightPartition = singletonPartitions(CandidateSet.from(right))
        checkSwapCandidate(candidateContext, left, right, contextPartition, leftPartition, rightPartition)
      }.toSeq
      val remainingChecks = checks -- candidatePartitions.keys
      if(remainingChecks.isEmpty) {
        processResults(validODs)
      } else
        performChecks(singletonPartitions, remainingChecks, validODs)
    }
  }

  private def processResults(validODs: Seq[EquivalencyOrderDependency]): Behavior[Command] = {
    if (validODs.nonEmpty) {
      context.log.trace("Found valid candidates: {}", validODs.mkString(", "))
      rsProxy ! FoundDependencies(validODs)
    } else {
      context.log.trace("No valid equivalency candidates found")
    }

    val enrichedIsValid = isValid(validODs, _)
    val removedCandidates = swapCandidates.filter(enrichedIsValid)

    next(removedCandidates)
  }
}
