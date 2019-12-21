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
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.worker.Worker
import com.github.codelionx.distod.actors.worker.Worker.{CheckSplitCandidates, CheckSwapCandidates}
import com.github.codelionx.distod.actors.LeaderGuardian
import com.github.codelionx.distod.actors.master.Master.{Command, LocalPeers}
import com.github.codelionx.distod.actors.master.MasterHelper.{GenerateSplitCandidates, GenerateSwapCandidates}
import com.github.codelionx.distod.actors.partitionMgmt.PartitionReplicator.PrimaryPartitionManager
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultCommand
import com.github.codelionx.util.trie.CandidateTrie


object Master {

  sealed trait Command
  final case class DispatchWork(replyTo: ActorRef[Worker.Command]) extends Command with CborSerializable
  final case class SplitCandidatesChecked(id: CandidateSet, removedSplitCandidates: CandidateSet)
    extends Command with CborSerializable
  final case class SwapCandidatesChecked(id: CandidateSet, removedSwapCandidates: Seq[(Int, Int)])
    extends Command with CborSerializable
  final case class NewCandidates(
      id: CandidateSet,
      jobType: JobType.JobType,
      stateUpdate: CandidateState.Delta
  ) extends Command with CborSerializable
  final case class GetPrimaryPartitionManager(replyTo: ActorRef[PrimaryPartitionManager])
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
}


class Master(context: ActorContext[Command], stash: StashBuffer[Command], localPeers: LocalPeers) {

  import Master._
  import localPeers._

  private val settings = Settings(context.system)
  private val helperPool: ActorRef[MasterHelper.Command] = context.spawn(
    MasterHelper.pool(settings.numberOfWorkers, context.self),
    MasterHelper.poolName
  )
  private val state: CandidateTrie[CandidateState] = CandidateTrie.empty

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

      case GetPrimaryPartitionManager(replyTo) =>
        replyTo ! PrimaryPartitionManager(partitionManager)
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

        state ++= rootCandidateState ++ L1candidateState

        // prepare L2 nodes:
        val suffixCandidateSets = attributes.map(CandidateSet.from(_))
        for(l1Node <- L1candidateState.keySet) {
          val prefixedState = state.withPrefix(l1Node)
          val prefixId = prefixedState(CandidateSet.empty).id
          for(a <- attributes if a > prefixId.head) {
            // L1 nodes do not perform swap checks, so the preconditions for L2 nodes and swap checks are already met
            val state = CandidateState(prefixId + a).incSwapPreconditions.incSwapPreconditions
            prefixedState.update(CandidateSet.from(a), state)
          }
        }
        val initialQueue = L1candidateState.keys.map(key => key -> JobType.Split)
        context.log.info("Master ready, initial work queue: {}", initialQueue)
        context.log.info("initial state: {}", state.mkString(", "))
        stash.unstashAll(
          behavior(attributes, suffixCandidateSets, WorkQueue.from(initialQueue), 0)
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
      attributeCandidateSets: Seq[CandidateSet],
//      state: Map[CandidateSet, CandidateState],
      workQueue: WorkQueue,
      testedCandidates: Int
  ): Behavior[Command] = Behaviors.receiveMessage {
    case GetPrimaryPartitionManager(replyTo) =>
      replyTo ! PrimaryPartitionManager(partitionManager)
      Behaviors.same

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
      }
      behavior(attributes, attributeCandidateSets, newWorkQueue, testedCandidates)

    case SplitCandidatesChecked(id, removedSplitCandidates) =>
      val job = id -> JobType.Split
      val stateUpdate = CandidateState.SplitChecked(removedSplitCandidates)
      updateStateAndNext(attributes, attributeCandidateSets, workQueue, testedCandidates + 1, job, stateUpdate)

    case SwapCandidatesChecked(id, removedSwapCandidates) =>
      val job = id -> JobType.Swap
      val stateUpdate = CandidateState.SwapChecked(removedSwapCandidates)
      updateStateAndNext(attributes, attributeCandidateSets, workQueue, testedCandidates + 1, job, stateUpdate)

    case NewCandidates(id, jobType, stateUpdate) =>
      val job = id -> jobType
      state.updateWith(id){
        case None => Some(CandidateState.createFromDelta(id, stateUpdate))
        case Some(s) => Some(s.updated(stateUpdate))
      }
      context.log.info("Received new candidates for job {}", job)
      val newQueue = workQueue.enqueue(job).removePendingGeneration(job)
      stash.unstashAll(
        behavior(attributes, attributeCandidateSets, newQueue, testedCandidates)
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
      CandidateSet.empty -> CandidateState.forL0(CandidateSet.empty, CandidateSet.fromSpecific(attributes))
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
      candidate -> CandidateState.forL1(candidate, CandidateSet.fromSpecific(attributes))
    }
    L1candidates.zipWithIndex.foreach { case (candidate, index) =>
      partitionManager ! InsertPartition(candidate, partitions(index))
    }
    L1candidateState.toMap
  }

  private def updateStateAndNext(
      attributes: Seq[Int],
      attributeCandidateSets: Seq[CandidateSet],
      workQueue: WorkQueue,
      testedCandidates: Int,
      job: (CandidateSet, JobType.JobType),
      stateUpdate: CandidateState.Delta
  ): Behavior[Command] = {
    context.log.info("Received results for {}", job)
    val (id, jobType) = job
    // update state based on received results
    val newWorkQueue = workQueue.removePending(job)
    val newTaskState = state.updateIfDefinedWith(id)(_.updated(stateUpdate))

    // update successors and get new generation jobs
    val successors = id.successors(attributes.toSet)
    val nodeIsPruned = newTaskState.exists(_.isPruned)
    val newGenerationJobs =
      if(nodeIsPruned) {
        // node pruning! --> invalidate all successing nodes
        successors.foreach(s =>
          state.remove(s)
        )
        Seq.empty
      } else {
        // update counters of successors and send new generation jobs
        successors.flatMap { successor =>
          val oldState = state.getOrElse(successor, CandidateState(successor))
          val successorState = oldState.incPreconditions(jobType)
          state.update(successor, successorState)
          // only check split readiness if we changed the split preconditions (otherwise swap updates would also trigger
          // the new generation of split candidates)
          val splitReady =
            if(jobType == JobType.Split) successorState.isReadyToCheck(JobType.Split)
            else false
          val swapReady = successorState.isReadyToCheck(JobType.Swap)

          // isReadyToCheck should only be true once
          val splitJobs =
            if(splitReady /*&& !workQueue.containsPending(successorState.id -> JobType.GenerateSplit)*/) {
              // FIXME: make immutable View of state
              helperPool ! GenerateSplitCandidates(successorState.id, state)
              Seq(successorState.id -> JobType.Split)
            } else {
              Seq.empty
            }
          val swapJobs =
            if(swapReady /*&& !workQueue.containsPending(successorState.id -> JobType.GenerateSwap)*/) {
              // FIXME: make immutable view of state
              helperPool ! GenerateSwapCandidates(successorState.id, state)
              Seq(successorState.id -> JobType.Swap)
            } else {
              Seq.empty
            }
          splitJobs ++ swapJobs
        }
      }
    val newWorkQueue2 = newWorkQueue.addPendingGenerationAll(newGenerationJobs)
    behavior(attributes, attributeCandidateSets, newWorkQueue2, testedCandidates + 1)
  }

}
