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
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates, GenerateCandidates}
import com.github.codelionx.distod.actors.LeaderGuardian
import com.github.codelionx.distod.actors.master.Master.{Command, LocalPeers}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultCommand


object Master {

  sealed trait Command
  final case class DispatchWork(replyTo: ActorRef[Worker.Command]) extends Command with CborSerializable
  final case class SplitCandidatesChecked(id: CandidateSet, removedSplitCandidates: CandidateSet)
    extends Command with CborSerializable
  final case class SwapCandidatesChecked(id: CandidateSet, removedSwapCandidates: Seq[(Int, Int)])
    extends Command with CborSerializable
  final case class NewCandidates(
      id: CandidateSet,
      newJobs: Iterable[(CandidateSet, JobType.JobType)],
      stateUpdates: Iterable[(CandidateSet, CandidateState.Delta)]
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

    Behaviors.receiveMessagePartial[Command] {
      case m: DispatchWork =>
        context.log.debug("Worker {} is ready for work, stashing request", m.replyTo)
        stash.stash(m)
        Behaviors.same

      case WrappedLoadingEvent(PartitionsLoaded(table @ PartitionedTable(name, headers, partitions))) =>
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
        val initialQueue = L1candidateState.keys.map(key => key -> JobType.Split)
        context.log.info("Master ready, initial work queue: {}", initialQueue)
        stash.unstashAll(
          behavior(attributes, state, WorkQueue.from(initialQueue), 0)
        )
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
      workQueue: WorkQueue,
      testedCandidates: Int
  ): Behavior[Command] = Behaviors.receiveMessage {
    case DispatchWork(_) if workQueue.isEmpty =>
      context.log.debug("Request for work, but no more work available and no pending requests: algo finished!")
      finished(testedCandidates)

    case m: DispatchWork if workQueue.noWork && workQueue.hasPending =>
      context.log.debug("Stashing request for work from {}", m.replyTo)
      stash.stash(m)
      Behaviors.same

    case DispatchWork(replyTo) if workQueue.hasWork =>
      val ((taskId, jobType), newWorkQueue) = workQueue.dequeue()
      val taskState = state(taskId)
      context.log.debug("Dispatching task {} {} to {}", jobType, taskId, replyTo)
      jobType match {
        case JobType.Split =>
          val splitCandidates = taskId & taskState.splitCandidates
          replyTo ! CheckSplitCandidates(taskId, splitCandidates)
        case JobType.Swap =>
          val swapCandidates = taskState.swapCandidates
          replyTo ! CheckSwapCandidates(taskId, swapCandidates)
        case JobType.Generation =>
          replyTo ! GenerateCandidates(taskId, state, newWorkQueue)
      }
      behavior(attributes, state, newWorkQueue, testedCandidates)

    case SplitCandidatesChecked(id, removedSplitCandidates) =>
      val taskState = state(id)
      val updatedTaskState = taskState.copy(
        splitCandidates = taskState.splitCandidates -- removedSplitCandidates,
        splitChecked = true
      )
      val newWorkQueue = workQueue.removePending(id, JobType.Split)
      updateStateAndNext(attributes, state, newWorkQueue, id, updatedTaskState, testedCandidates + 1)

    case SwapCandidatesChecked(id, removedSwapCandidates) =>
      val taskState = state(id)
      val updatedTaskState = taskState.copy(
        swapCandidates = taskState.swapCandidates.filterNot(removedSwapCandidates.contains),
        swapChecked = true
      )
      val newWorkQueue = workQueue.removePending(id, JobType.Swap)
      updateStateAndNext(attributes, state, newWorkQueue, id, updatedTaskState, testedCandidates + 1)

    case NewCandidates(id, newJobs, stateUpdates) =>
      // remove duplicate results (caused by workers having old copy of the state and queue)
      val filteredJobs = newJobs.filterNot(workQueue.contains)
      val updatedState = stateUpdates
        // new candidates should be the same as before and if we already checked the candidate, we will ignore the update
//        .filterNot {
//          case (id, CandidateState.NewSwapCandidates(_)) => workQueue.contains(id -> JobType.Swap)
//          case (id, CandidateState.NewSplitCandidates(_)) => workQueue.contains(id -> JobType.Split)
//        }
        .foldLeft(state) { case (acc, (id, delta)) =>
          acc.get(id) match {
            case None => acc.updated(id, CandidateState.createFromDelta(delta))
            case Some(s) => acc.updated(id, s.updated(delta))
          }
        }
      context.log.info("Received new jobs: {}", newJobs.mkString(", "))
      val newQueue = workQueue.removePending(id -> JobType.Generation).enqueueAll(filteredJobs)
      stash.unstashAll(
        behavior(attributes, updatedState, newQueue, testedCandidates)
      )

    case m =>
      context.log.warn("Received unexpected message: {}", m)
      Behaviors.same
  }

  private def finished(testedCandidates: Int): Behavior[Command] = {
    println(s"Tested candidates: $testedCandidates")
    guardian ! LeaderGuardian.AlgorithmFinished
    Behaviors.receiveMessage {
      case DispatchWork(_) =>
        // can be ignored without issues, because we are out of work
        Behaviors.same
      case m =>
        context.log.warn("Ignoring message because we are in shutdown: {}", m)
        Behaviors.same
    }
  }

  private def generateLevel0(attributes: Seq[Int], nTuples: Int): Map[CandidateSet, CandidateState] = {
    val L0CandidateState = Map(
      CandidateSet.empty -> CandidateState.forL0(CandidateSet.fromSpecific(attributes))
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
      candidate -> CandidateState.forL1(CandidateSet.fromSpecific(attributes))
    }
    L1candidates.zipWithIndex.foreach { case (candidate, index) =>
      partitionManager ! InsertPartition(candidate, partitions(index))
    }
    L1candidateState.toMap
  }

  private def updateStateAndNext(
      attributes: Seq[Int],
      state: Map[CandidateSet, CandidateState],
      workQueue: WorkQueue,
      id: CandidateSet,
      newTaskState: CandidateState,
      testedCandidates: Int
  ): Behavior[Command] = {
    val newState = state + (id -> newTaskState)
    if (id == CandidateSet.from(0, 1, 2, 3)) {
      println("DEBUG")
    }
    // node pruning
    val pruneNode = newTaskState.splitCandidates.isEmpty && newTaskState.swapCandidates.isEmpty
    val newWorkQueue =
      if (!pruneNode && !workQueue.containsPending(id -> JobType.Generation)) {
        workQueue.enqueue(id -> JobType.Generation)
      } else {
        workQueue
      }
    context.log.info("Received results for {}", id)
    behavior(attributes, newState, newWorkQueue, testedCandidates)
  }

}
