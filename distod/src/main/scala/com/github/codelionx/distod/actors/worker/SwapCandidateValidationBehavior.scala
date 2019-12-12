package com.github.codelionx.distod.actors.worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.worker.Worker.{Command, WrappedPartitionEvent}
import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.FoundDependencies
import com.github.codelionx.distod.types.CandidateSet


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


  def start(): Behavior[Command] = {
    context.log.debug("Loading partitions for all swap checks")

    val distinctAttributes = swapCandidates.flatMap { case (attr1, attr2) => Seq(attr1, attr2) }.distinct
    val singletonPartitionKeys = distinctAttributes.map(attribute => CandidateSet.from(attribute))
    singletonPartitionKeys.foreach(attribute =>
      partitionManager ! LookupPartition(attribute, partitionEventMapper)
    )
    for ((a, b) <- swapCandidates) {
      val fdContext = candidateId - a - b
      partitionManager ! LookupStrippedPartition(fdContext, partitionEventMapper)
    }

    collectPartitions(Map.empty, Map.empty, singletonPartitionKeys.size, swapCandidates.size)
  }

  def collectPartitions(
      singletonPartitions: Map[CandidateSet, FullPartition],
      candidatePartitions: Map[CandidateSet, StrippedPartition],
      expectedSingletons: Int,
      expectedCandidates: Int
  ): Behavior[Command] = {
    val nextBehavior = (singletonPartitions: Map[CandidateSet, FullPartition], candidatePartitions: Map[CandidateSet, StrippedPartition]) => {
      if (singletonPartitions.size == expectedSingletons && candidatePartitions.size == expectedCandidates) {
        performCheck(singletonPartitions, candidatePartitions)
      } else {
        collectPartitions(singletonPartitions, candidatePartitions, expectedSingletons, expectedCandidates)
      }
    }

    Behaviors.receiveMessagePartial {
      case WrappedPartitionEvent(PartitionFound(key, value)) =>
        context.log.debug("Received full partition {}", key)
        val newPartitions = singletonPartitions + (key -> value)
        nextBehavior(newPartitions, candidatePartitions)

      case WrappedPartitionEvent(StrippedPartitionFound(key, value)) =>
        context.log.debug("Received stripped partition {}", key)
        val newPartitions = candidatePartitions + (key -> value)
        nextBehavior(singletonPartitions, newPartitions)

    }
  }

  def performCheck(
      singletonPartitions: Map[CandidateSet, FullPartition], candidatePartitions: Map[CandidateSet, StrippedPartition]
  ): Behavior[Command] = {
    val result = checkSwapCandidates(candidateId, swapCandidates, singletonPartitions, candidatePartitions)

    if (result.validOds.nonEmpty) {
      context.log.debug("Found valid candidates: {}", result.validOds.mkString(", "))
      rsProxy ! FoundDependencies(result.validOds)
    } else {
      context.log.debug("No valid equivalency candidates found")
    }

    next(result.removedCandidates)
  }
}
