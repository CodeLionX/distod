package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import com.github.codelionx.distod.actors.Master.{Command, LocalPeers}
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.{PartitionManagementProtocol, ResultCollectionProtocol}
import com.github.codelionx.distod.protocols.DataLoadingProtocol._
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.worker.Worker
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultCommand

import scala.collection.immutable.{BitSet, Queue}


object Master {

  sealed trait Command
  final case class DispatchWork(replyTo: ActorRef[Worker.Command]) extends Command with CborSerializable
  final case class CandidateNodeChecked(
      id: CandidateSet,
      jobType: JobType.JobType,
      removedSplitCandidates: CandidateSet,
      removedSwapCandidates: Seq[(Int, Int)]
  ) extends Command with CborSerializable
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
      splitCandidates: Seq[Int],
      swapCandidates: Seq[(Int, Int)],
      isValid: Boolean
  )

  object JobType {
    sealed trait JobType
    case object Split extends JobType
    case object Swap extends JobType
  }
}


class Master(context: ActorContext[Command], stash: StashBuffer[Command], localPeers: LocalPeers) {

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

        // testPartitionMgmt()
        val testSwapState = Map(CandidateSet.from(2, 3) -> CandidateState(
          splitCandidates = Seq.empty,
          swapCandidates = Seq(2 -> 3),
          isValid = true
        ))
        val testSwapQueue = Queue(CandidateSet.from(2, 3) -> JobType.Swap)

        val state = rootCandidateState ++ L1candidateState ++ testSwapState
        val initialQueue = L1candidateState.keys.map(key => key -> JobType.Split).to(Queue) ++ testSwapQueue
        context.log.info("Master ready, initial work queue: {}", initialQueue)
        stash.unstashAll(
          behavior(state, initialQueue, Set.empty)
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
      state: Map[CandidateSet, CandidateState],
      workQueue: Queue[(CandidateSet, JobType.JobType)],
      pending: Set[(CandidateSet, JobType.JobType)]
  ): Behavior[Command] = Behaviors.receiveMessage {
    case DispatchWork(_) if workQueue.isEmpty && pending.isEmpty =>
      finished()

    case m: DispatchWork if workQueue.isEmpty && pending.nonEmpty =>
      stash.stash(m)
      Behaviors.same

    case DispatchWork(replyTo) if workQueue.nonEmpty =>
      val ((taskId, jobType), newWorkQueue) = workQueue.dequeue
      val taskState = state(taskId)
      jobType match {
        case JobType.Split =>
          val splitCandidates = taskId & BitSet.fromSpecific(taskState.splitCandidates)
          replyTo ! CheckSplitCandidates(taskId, splitCandidates)
        case JobType.Swap =>
          val swapCandidates = taskState.swapCandidates
          replyTo ! CheckSwapCandidates(taskId, swapCandidates)
      }
      behavior(state, newWorkQueue, pending + (taskId -> jobType))

    case CandidateNodeChecked(id, jobType, removedSplitCandidates, removedSwapCandidates) =>
      val taskState = state(id)
      println("Initial state", id, taskState)
      println("Received updated candidates:",
        CandidateSet.fromSpecific(taskState.splitCandidates) -- removedSplitCandidates,
        taskState.swapCandidates.filterNot(removedSwapCandidates.contains)
      )
      behavior(state, workQueue, pending - (id -> jobType))

    case m =>
      context.log.info("Received message: {}", m)
      Behaviors.same
  }

  private def finished(): Behavior[Command] = {
    guardian ! LeaderGuardian.AlgorithmFinished
    Behaviors.same
  }

  private def generateLevel0(attributes: Seq[Int], nTuples: Int): Map[CandidateSet, CandidateState] = {
    val L0CandidateState = Map(
      CandidateSet.empty -> CandidateState(
        splitCandidates = attributes,
        swapCandidates = Seq.empty,
        isValid = true
      )
    )
    partitionManager ! InsertPartition(CandidateSet.empty, StrippedPartition(
      numberElements = nTuples,
      numberClasses = 1,
      equivClasses = IndexedSeq((0 until nTuples).toSet)
    ))

    L0CandidateState
  }

  private def generateLevel1(
      attributes: Seq[Int], partitions: Array[FullPartition]
  ): Map[CandidateSet, CandidateState] = {
    val L1candidates = attributes.map(columnId => CandidateSet.from(columnId))
    val L1candidateState = L1candidates.map { candidate =>
      candidate -> CandidateState(
        splitCandidates = attributes,
        swapCandidates = Seq.empty,
        isValid = true
      )
    }
    L1candidates.zipWithIndex.foreach { case (candidate, index) =>
      partitionManager ! InsertPartition(candidate, partitions(index))
    }
    L1candidateState.toMap
  }
}
