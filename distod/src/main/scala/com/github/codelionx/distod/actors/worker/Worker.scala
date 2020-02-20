package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.MasterHelper
import com.github.codelionx.distod.actors.master.MasterHelper.{DispatchWork, SplitCandidatesChecked, SwapCandidatesChecked}
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.partitions.Partition
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{FoundDependencies, ResultProxyCommand}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.timing.Timing


object Worker {

  sealed trait Command extends CborSerializable

  final case class CheckSplitCandidates(candidateId: CandidateSet, splitCandidates: CandidateSet) extends Command
  final case class CheckSwapCandidates(candidateId: CandidateSet, swapCandidates: Seq[(Int, Int)]) extends Command
  private[worker] case class WrappedPartitionEvent(event: PartitionEvent) extends Command
  private[worker] case object Stop extends Command

  def name(n: Int): String = s"worker-$n"

  def apply(
      partitionManager: ActorRef[PartitionCommand],
      rsProxy: ActorRef[ResultProxyCommand],
      master: ActorRef[MasterHelper.Command]
  ): Behavior[Command] =
    Behaviors.setup[Command](context =>
      Behaviors.withStash[Command](10) { stash =>
        val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))
        new Worker(WorkerContext(context, stash, master, partitionManager, rsProxy, partitionEventMapper)).start()
      }
    )
}


class Worker(workerContext: WorkerContext) extends CandidateGeneration {

  import Worker._
  import workerContext._


  private val timing = Timing(context.system)
  private var jobs: Map[CandidateSet, CheckJob] = Map.empty

  def start(): Behavior[Command] = initialize()

  private def initialize(): Behavior[Command] = {
    partitionManager ! LookupAttributes(partitionEventMapper)

    Behaviors.receiveMessagePartial {
      case WrappedPartitionEvent(AttributesFound(attributes)) =>
        context.log.trace("Worker ready to process candidates: Master={}, Attributes={}", master, attributes)
        master ! DispatchWork(context.self)
        behavior(attributes)

      case Stop =>
        Behaviors.stopped
    }
  }

  private def behavior(attributes: Seq[Int], stopped: Boolean = false): Behavior[Command] = Behaviors.receiveMessage {
    case CheckSplitCandidates(candidateId, splitCandidates) =>
      context.log.debug("Checking split candidates of node {}", candidateId)

      val job = new CheckSplitJob(candidateId, splitCandidates)
      context.log.trace("Loading partition errors for split checks")
      for (c <- job.errorIds) {
        partitionManager ! LookupError(candidateId, c, partitionEventMapper)
      }
      jobs += candidateId -> job
      Behaviors.same

    case CheckSwapCandidates(candidateId, swapCandidates) =>
      context.log.debug("Checking swap candidates of node {}", candidateId)
      val job = new CheckSwapJob(candidateId, swapCandidates)

      context.log.trace("Loading partitions for swap checks")
      val singletonPartitionKeys = job.singletonPartitionKeys
      singletonPartitionKeys.foreach(attribute =>
        partitionManager ! LookupPartition(candidateId, attribute, partitionEventMapper)
      )
      val partitionKeys = job.partitionKeys
      partitionKeys.foreach(candidateContext =>
        partitionManager ! LookupStrippedPartition(candidateId, candidateContext, partitionEventMapper)
      )
      jobs += candidateId -> job
      Behaviors.same

    case WrappedPartitionEvent(ErrorFound(candidateId, key, value)) =>
      context.log.trace("Received partition error value: {}, {}", key, value)
      jobs.get(candidateId) match {
        case Some(job: CheckSplitJob) =>
          job.receivedError(key, value)

          val finished = timing.unsafeTime("Split check") {
            job.performPossibleChecks()
          }

          if (finished)
            processSplitResult(job, attributes, stopped)
          else
            Behaviors.same
        case _ =>
          context.log.error("Received error value for unknown job {}", candidateId)
          Behaviors.same
      }

    case WrappedPartitionEvent(PartitionFound(candidateId, key, value)) =>
      receivedPartition(candidateId, key, value, attributes, stopped)

    case WrappedPartitionEvent(StrippedPartitionFound(candidateId, key, value)) =>
      receivedPartition(candidateId, key, value, attributes, stopped)

    case Stop =>
      context.log.info("Finishing last job before stopping")
      behavior(attributes, stopped = true)

    case WrappedPartitionEvent(event) =>
      context.log.debug("Ignored {}", event)
      Behaviors.same
  }

  private def receivedPartition(
      candidateId: CandidateSet, key: CandidateSet, value: Partition, attributes: Seq[Int], stopped: Boolean
  ): Behavior[Command] = {
    context.log.trace("Received partition {}", key)
    jobs.get(candidateId) match {
      case Some(job: CheckSwapJob) =>
        job.receivedPartition(key, value)

        val finished = timing.unsafeTime("Swap check") {
          job.performPossibleChecks()
        }
        if (finished)
          processSwapResults(job, attributes, stopped)
        else
          Behaviors.same
      case _ =>
        context.log.error("Received partition for unknown job {}", candidateId)
        Behaviors.same
    }
  }

  private def processSplitResult(job: CheckSplitJob, attributes: Seq[Int], stopped: Boolean): Behavior[Command] = {
    val (validODs, removedCandidates) = job.results(attributes)
    val candidateId = job.candidateId
    if (validODs.nonEmpty) {
      context.log.trace("Found valid candidates: {}", validODs.mkString(", "))
      rsProxy ! FoundDependencies(validODs)
    } else {
      context.log.trace("No valid constant candidates found")
    }
    // notify master of result
    context.log.debug("Sending results of ({}, Split) to master at {}", candidateId, master)
    master ! SplitCandidatesChecked(candidateId, removedCandidates)
    jobs -= candidateId
    // ready to work on next node:
    next(attributes, stopped)
  }

  private def processSwapResults(job: CheckSwapJob, attributes: Seq[Int], stopped: Boolean): Behavior[Command] = {
    val (validODs, removedCandidates) = job.results(Seq.empty)
    val candidateId = job.candidateId
    if (validODs.nonEmpty) {
      context.log.trace("Found valid candidates: {}", validODs.mkString(", "))
      rsProxy ! FoundDependencies(validODs)
    } else {
      context.log.trace("No valid equivalency candidates found")
    }
    // notify master of result
    context.log.debug("Sending results of ({}, Swap) to master at {}", candidateId, master)
    master ! SwapCandidatesChecked(candidateId, removedCandidates)
    jobs -= candidateId

    // ready to work on next node:
    next(attributes, stopped)
  }

  private def next(attributes: Seq[Int], stopped: Boolean): Behavior[Command] =
    if (!stopped) {
      master ! DispatchWork(context.self)
      stash.unstashAll(
        behavior(attributes)
      )
    } else {
      Behaviors.stopped
    }
}