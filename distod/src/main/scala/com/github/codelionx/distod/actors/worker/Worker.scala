package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.{JobType, MasterHelper}
import com.github.codelionx.distod.actors.master.MasterHelper.{CancelWork, DispatchWork, SplitCandidatesChecked, SwapCandidatesChecked}
import com.github.codelionx.distod.discovery.CandidateGeneration
import com.github.codelionx.distod.partitions.Partition
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{FoundDependencies, ResultProxyCommand}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.Settings
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
    Behaviors.setup[Command] { context =>
      val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))
      partitionManager ! LookupAttributes(partitionEventMapper)

      Behaviors.receiveMessagePartial {
        case WrappedPartitionEvent(AttributesFound(attributes)) =>
          new Worker(WorkerContext(context, master, partitionManager, rsProxy, partitionEventMapper), attributes)
            .start()

        case Stop =>
          Behaviors.stopped
      }
    }
}


class Worker(workerContext: WorkerContext, attributes: Seq[Int]) extends CandidateGeneration {

  import Worker._
  import workerContext._


  private val timing = Timing(context.system)
  private val settings = Settings(context.system)
  private var splitJobs: Map[CandidateSet, CheckSplitJob] = Map.empty
  private var swapJobs: Map[CandidateSet, CheckSwapJob] = Map.empty


  private object StopBehaviorInterceptor extends WorkerStopInterceptor {

    override def dispatchWorkFromMaster(): Unit =
      if (!stopped) {
        master ! DispatchWork(context.self)
        openDispatches += 1
      } else {
        context.log.trace("Preventing the request for more work, because worker is in shut down phase")
      }

    override protected def cancelAll(): Unit = {
      for (job <- splitJobs.keys) {
        master ! CancelWork(job, JobType.Split)
      }
      for (job <- swapJobs.keys) {
        master ! CancelWork(job, JobType.Swap)
      }
    }

    override protected def cancel(candidateId: CandidateSet, jobType: JobType.JobType): Unit =
      master ! CancelWork(candidateId, jobType)
  }


  def start(): Behavior[Command] = {
    context.log.trace(
      "Worker ready to process candidates: Master={}, Attributes={}, requesting {} jobs",
      master,
      attributes,
      settings.concurrentWorkerJobs
    )
    for (_ <- 0 until settings.concurrentWorkerJobs) {
      StopBehaviorInterceptor.dispatchWorkFromMaster()
    }
    behavior()
  }

  private def behavior(): Behavior[Command] = Behaviors.intercept(() => StopBehaviorInterceptor)(
    Behaviors.receiveMessage {

      case CheckSplitCandidates(candidateId, splitCandidates) =>
        context.log.debug("Checking split candidates of node {}", candidateId)

        if (splitCandidates.nonEmpty) {
          val job = new CheckSplitJob(candidateId, splitCandidates)

          context.log.trace("Loading partition errors for split checks")
          for (c <- job.errorIds) {
            partitionManager ! LookupError(candidateId, c, partitionEventMapper)
          }
          splitJobs += candidateId -> job
          Behaviors.same
        } else {
          // notify master of result
          context.log.debug("Sending results of ({}, Split) to master at {}", candidateId, master)
          master ! SplitCandidatesChecked(candidateId, CandidateSet.empty)

          // ready to work on next node:
          StopBehaviorInterceptor.dispatchWorkFromMaster()
          Behaviors.same
        }

      case CheckSwapCandidates(candidateId, swapCandidates) =>
        context.log.debug("Checking swap candidates of node {}", candidateId)

        if (swapCandidates.nonEmpty) {
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
          swapJobs += candidateId -> job
          Behaviors.same
        } else {
          // notify master of result
          context.log.debug("Sending results of ({}, Swap) to master at {}", candidateId, master)
          master ! SwapCandidatesChecked(candidateId, Seq.empty)

          // ready to work on next node:
          StopBehaviorInterceptor.dispatchWorkFromMaster()
          Behaviors.same
        }

      case WrappedPartitionEvent(ErrorFound(candidateId, key, value)) =>
        context.log.trace("Received partition error value: {}, {}", key, value)
        splitJobs.get(candidateId) match {
          case Some(job) =>
            job.receivedError(key, value)

            val finished = timing.unsafeTime("Split check") {
              job.performPossibleChecks()
            }
            if (finished)
              processSplitResults(job, attributes)

          case None =>
            context.log.error("Received error for unknown job {}", candidateId)
        }
        Behaviors.same

      case WrappedPartitionEvent(PartitionFound(candidateId, key, value)) =>
        receivedPartition(candidateId, key, value, attributes)
        Behaviors.same

      case WrappedPartitionEvent(StrippedPartitionFound(candidateId, key, value)) =>
        receivedPartition(candidateId, key, value, attributes)
        Behaviors.same

      case WrappedPartitionEvent(event) =>
        context.log.debug("Ignored {}", event)
        Behaviors.same

      case Stop =>
        // is caught by interceptor
        Behaviors.same
    }
  )

  private def receivedPartition(
      candidateId: CandidateSet, key: CandidateSet, value: Partition, attributes: Seq[Int]
  ): Unit = {
    context.log.trace("Received partition {} for {}", key, candidateId)
    swapJobs.get(candidateId) match {
      case Some(job) =>
        job.receivedPartition(key, value)

        val finished = timing.unsafeTime("Swap check") {
          job.performPossibleChecks()
        }
        if (finished)
          processSwapResults(job)

      case None =>
        context.log.error("Received partition for unknown job {}", candidateId)
    }
  }

  private def processSplitResults(job: CheckSplitJob, attributes: Seq[Int]): Unit = {
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

    // ready to work on next node:
    splitJobs -= candidateId
    StopBehaviorInterceptor.dispatchWorkFromMaster()
  }

  private def processSwapResults(job: CheckSwapJob): Unit = {
    val (validODs, removedCandidates) = job.results
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

    // ready to work on next node:
    swapJobs -= candidateId
    StopBehaviorInterceptor.dispatchWorkFromMaster()
  }
}
