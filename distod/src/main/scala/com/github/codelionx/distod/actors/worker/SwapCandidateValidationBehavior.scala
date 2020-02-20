package com.github.codelionx.distod.actors.worker

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.actors.worker.Worker.{Command, WrappedPartitionEvent}
import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.partitions.Partition
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
  private val job = new CheckSwapJob(candidateId, swapCandidates)

  def start(): Behavior[Command] = {
    context.log.trace("Loading partitions for swap checks")

    val singletonPartitionKeys = job.singletonPartitionKeys
    singletonPartitionKeys.foreach(attribute =>
      partitionManager ! LookupPartition(candidateId, attribute, partitionEventMapper)
    )
    val partitionKeys = job.partitionKeys
    partitionKeys.foreach(candidateContext =>
      partitionManager ! LookupStrippedPartition(candidateId, candidateContext, partitionEventMapper)
    )

    behavior()
  }

  private def behavior(): Behavior[Command] = Behaviors.receiveMessage {
    case WrappedPartitionEvent(PartitionFound(_, key, value)) =>
      receivedPartition(key, value)

    case WrappedPartitionEvent(StrippedPartitionFound(_, key, value)) =>
      receivedPartition(key, value)

    case m =>
      stash.stash(m)
      Behaviors.same
  }

  private def receivedPartition(key: CandidateSet, value: Partition): Behavior[Command] = {
    context.log.trace("Received partition {}", key)
    job.receivedPartition(key, value)

    val finished = timing.unsafeTime("Swap check") {
      job.performPossibleChecks()
    }
    if (finished) {
      val (validODs, removedCandidates) = job.results(Seq.empty)
      processResults(validODs, removedCandidates)
    } else {
      Behaviors.same
    }
  }

  private def processResults(
      validODs: Seq[EquivalencyOrderDependency], removedCandidates: Seq[(Int, Int)]
  ): Behavior[Command] = {
    if (validODs.nonEmpty) {
      context.log.trace("Found valid candidates: {}", validODs.mkString(", "))
      rsProxy ! FoundDependencies(validODs)
    } else {
      context.log.trace("No valid equivalency candidates found")
    }
    next(removedCandidates)
  }
}
