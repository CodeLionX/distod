package com.github.codelionx.distod.actors.master

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.{PartitionManagementProtocol, ResultCollectionProtocol}
import com.github.codelionx.distod.protocols.DataLoadingProtocol._
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.worker.Worker
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates}
import com.github.codelionx.distod.actors.LeaderGuardian
import com.github.codelionx.distod.actors.master.Master.{Command, LocalPeers}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultCommand

import scala.collection.immutable.Queue


object Master {

  sealed trait Command
  final case class DispatchWork(replyTo: ActorRef[Worker.Command]) extends Command with CborSerializable
  final case class SplitCandidatesChecked(id: CandidateSet, removedSplitCandidates: CandidateSet)
    extends Command with CborSerializable
  final case class SwapCandidatesChecked(id: CandidateSet, removedSwapCandidates: Seq[(Int, Int)])
    extends Command with CborSerializable
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command
  private final case class WrappedPartitionEvent(e: PartitionManagementProtocol.PartitionEvent) extends Command

  val MasterServiceKey: ServiceKey[Command] = ServiceKey("master")

  val name = "master"

  def apply(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand],
      resultCollector: ActorRef[ResultCommand]
  ): Behavior[Command] = Behaviors.setup(context =>
    Behaviors.withStash(100) { stash =>
      new Master(context, stash, LocalPeers(guardian, dataReader, partitionManager, resultCollector)).start()
    })

  case class LocalPeers(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand],
      resultCollector: ActorRef[ResultCommand]
  )
  case class CandidateState(
      splitCandidates: CandidateSet,
      swapCandidates: Seq[(Int, Int)],
      splitChecked: Boolean = false,
      swapChecked: Boolean = false
  )

  object JobType {
    sealed trait JobType
    case object Split extends JobType
    case object Swap extends JobType
  }
}


class Master(context: ActorContext[Command], stash: StashBuffer[Command], localPeers: LocalPeers)
  extends CandidateGeneration {

  import Master._
  import localPeers._


  def start(): Behavior[Command] = initialize()

  private def initialize(): Behavior[Command] = Behaviors.setup { context =>
    // register message adapters
    val loadingEventMapper = context.messageAdapter(e => WrappedLoadingEvent(e))

    // make master available to the whole cluster using the registry
    context.system.receptionist ! Receptionist.Register(MasterServiceKey, context.self)

    dataReader ! LoadPartitions(loadingEventMapper)

    def onLoadingEvent(loadingEvent: DataLoadingEvent): Behavior[Command] = loadingEvent match {
      case PartitionsLoaded(table @ PartitionedTable(name, headers, partitions)) =>
        context.log.info("Finished loading dataset {} with headers: {}", name, headers.mkString(","))

        // stop data reader to free up resources
        dataReader ! Stop

        val attributes = 0 until table.nAttributes
        partitionManager ! PartitionManagementProtocol.SetAttributes(attributes)
        resultCollector ! ResultCollectionProtocol.SetAttributeNames(headers.toIndexedSeq)

        // L0: root candidate node
        val rootCandidateState = generateLevel0(attributes, table.nTuples)

        // L1: single attribute candidate nodes
        val L1candidateState = generateLevel1(attributes, partitions)

        // TODO: remove
//         testPartitionMgmt()

        val state = rootCandidateState ++ L1candidateState
        val initialQueue = L1candidateState.keys.map(key => key -> JobType.Split).to(Queue)
        context.log.info("Master ready, initial work queue: {}", initialQueue)
        stash.unstashAll(
          behavior(attributes, state, initialQueue, Set.empty, 0)
        )
    }

    Behaviors.receiveMessagePartial[Command] {
      case m: DispatchWork =>
        context.log.debug("Woker {} is ready for work, stashing request", m.replyTo)
        stash.stash(m)
        Behaviors.same

      case WrappedLoadingEvent(event) =>
        onLoadingEvent(event)
    }
  }

  private def testPartitionMgmt(): Behavior[Command] = {
    val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))

    // ask for advanced partitions as a test
    partitionManager ! LookupError(CandidateSet.from(0, 1), partitionEventMapper)
    partitionManager ! LookupStrippedPartition(CandidateSet.from(0, 1, 2), partitionEventMapper)

    def onPartitionEvent(event: PartitionEvent): Behavior[Command] = event match {
      case ErrorFound(key, error) =>
        println("Partition error", key, error)
        Behaviors.same
      case PartitionFound(key, value) =>
        println("Received partition", key, value)
        Behaviors.same
      case StrippedPartitionFound(key, value) =>
        println("Received stripped partition", key, value)
        Behaviors.same

      // FINISHED for now
//        finished()
    }

    Behaviors.receiveMessagePartial {
      case m: DispatchWork =>
        stash.stash(m)
        Behaviors.same

      case WrappedPartitionEvent(event) =>
        onPartitionEvent(event)
    }
  }

  private def behavior(
      attributes: Seq[Int],
      state: Map[CandidateSet, CandidateState],
      workQueue: Queue[(CandidateSet, JobType.JobType)],
      pending: Set[(CandidateSet, JobType.JobType)],
      testedCandidates: Int
  ): Behavior[Command] = Behaviors.receiveMessage {
    case DispatchWork(_) if workQueue.isEmpty && pending.isEmpty =>
      println(s"Tested candidates: $testedCandidates")
      finished()

    case m: DispatchWork if workQueue.isEmpty && pending.nonEmpty =>
      context.log.debug("Stashing request for work from {}", m.replyTo)
      stash.stash(m)
      Behaviors.same

    case DispatchWork(replyTo) if workQueue.nonEmpty =>
      val ((taskId, jobType), newWorkQueue) = workQueue.dequeue
      val taskState = state(taskId)
      context.log.debug("Dispatching task {} {} to {}", jobType, taskId, replyTo)
      jobType match {
        case JobType.Split =>
          val splitCandidates = taskId & taskState.splitCandidates
          replyTo ! CheckSplitCandidates(taskId, splitCandidates)
        case JobType.Swap =>
          val swapCandidates = taskState.swapCandidates
          replyTo ! CheckSwapCandidates(taskId, swapCandidates)
      }
      behavior(attributes, state, newWorkQueue, pending + (taskId -> jobType), testedCandidates)

    case SplitCandidatesChecked(id, removedSplitCandidates) =>
      val taskState = state(id)
      val updatedTaskState = taskState.copy(
        splitCandidates = taskState.splitCandidates -- removedSplitCandidates,
        splitChecked = true
      )
      val removedPendingNode = id -> JobType.Split
      updateStateAndNext(attributes, state, workQueue, pending, id, updatedTaskState, removedPendingNode, testedCandidates + 1)

    case SwapCandidatesChecked(id, removedSwapCandidates) =>
      val taskState = state(id)
      val updatedTaskState = taskState.copy(
        swapCandidates = taskState.swapCandidates.filterNot(removedSwapCandidates.contains),
        swapChecked = true
      )
      val removedPendingNode = id -> JobType.Swap
      updateStateAndNext(attributes, state, workQueue, pending, id, updatedTaskState, removedPendingNode, testedCandidates + 1)

    case m =>
      context.log.warn("Received unexpected message: {}", m)
      Behaviors.same
  }

  private def finished(): Behavior[Command] = {
    guardian ! LeaderGuardian.AlgorithmFinished
    Behaviors.same
  }

  private def generateLevel0(attributes: Seq[Int], nTuples: Int): Map[CandidateSet, CandidateState] = {
    val L0CandidateState = Map(
      CandidateSet.empty -> CandidateState(
        splitCandidates = CandidateSet.fromSpecific(attributes),
        swapCandidates = Seq.empty,
        // we do not need to check for splits and swaps in level 0 (empty set)
        splitChecked = true,
        swapChecked = true
      )
    )
    partitionManager ! InsertPartition(CandidateSet.empty, StrippedPartition(
      nTuples = nTuples,
      numberElements = nTuples,
      numberClasses = 1,
      equivClasses = IndexedSeq((0 until nTuples).toSet)
    ))

    L0CandidateState
  }

  private def generateLevel1(
      attributes: Seq[Int],
      partitions: Array[FullPartition]
  ): Map[CandidateSet, CandidateState] = {
    val L1candidates = attributes.map(columnId => CandidateSet.from(columnId))
    val L1candidateState = L1candidates.map { candidate =>
      candidate -> CandidateState(
        splitCandidates = CandidateSet.fromSpecific(attributes),
        swapCandidates = Seq.empty,
        swapChecked = true // we do not need to check for swaps in level 1 (single attribute nodes)
      )
    }
    L1candidates.zipWithIndex.foreach { case (candidate, index) =>
      partitionManager ! InsertPartition(candidate, partitions(index))
    }
    L1candidateState.toMap
  }

  private def updateStateAndNext(
      attributes: Seq[Int],
      state: Map[CandidateSet, CandidateState],
      workQueue: Queue[(CandidateSet, JobType.JobType)],
      pending: Set[(CandidateSet, JobType.JobType)],
      id: CandidateSet,
      newTaskState: CandidateState,
      removedPending: (CandidateSet, JobType.JobType),
      testedCandidates: Int
  ): Behavior[Command] = {
    val newState = state + (id -> newTaskState)
    val newPending = pending - removedPending
    val (updatedState, newJobs) = generateNewCandidates(attributes, newState, workQueue, newPending, id)
    context.log.info("Received results for {}, new jobs: {}", removedPending, newJobs.mkString(", "))
    behavior(attributes, updatedState, workQueue.enqueueAll(newJobs), newPending, testedCandidates)
  }

}
