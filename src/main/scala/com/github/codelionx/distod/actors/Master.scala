package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import com.github.codelionx.distod.actors.Master.{Command, LocalPeers}
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.DataLoadingProtocol._
import com.github.codelionx.distod.protocols.PartitionManagementProtocol
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.Worker.CheckCandidateNode

import scala.collection.immutable.{BitSet, Queue}


object Master {

  sealed trait Command
  final case class DispatchWork(replyTo: ActorRef[Worker.Command]) extends Command with CborSerializable
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command
  private final case class WrappedPartitionEvent(e: PartitionManagementProtocol.PartitionEvent) extends Command

  val MasterServiceKey: ServiceKey[Command] = ServiceKey("master")

  val name = "master"

  def apply(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand]
  ): Behavior[Command] = Behaviors.setup(context =>
    Behaviors.withStash(100) { stash =>
      new Master(context, stash, LocalPeers(guardian, dataReader, partitionManager)).start()
    })

  case class LocalPeers(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand]
  )
  case class CandidateState(
      splitCandidates: Seq[Int],
      swapCandidates: Seq[(Int, Int)],
      isValid: Boolean
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

    def onLoadingEvent(loadingEvent: DataLoadingEvent): Behavior[Command] = loadingEvent match {
      case PartitionsLoaded(table @ PartitionedTable(name, headers, partitions)) =>
        context.log.info("Finished loading dataset {} with headers: {}", name, headers.mkString(","))

        // stop data reader to free up resources
        dataReader ! Stop

        val attributes = 0 until table.nAttributes
        partitionManager ! SetAttributes(attributes)

        // L0: root candidate node
        val rootCandidateState = generateLevel0(attributes, table.nTuples)

        // L1: single attribute candidate nodes
        val L1candidateState = generateLevel1(attributes, partitions)

        //        testPartitionMgmt()
        val state = rootCandidateState ++ L1candidateState
        val initialQueue = L1candidateState.keys.to(Queue)
        context.log.info("Master ready, initial work queue: {}", initialQueue)
        stash.unstashAll(
          behavior(state, initialQueue, Queue.empty)
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
      workQueue: Queue[CandidateSet],
      pendingQueue: Queue[CandidateSet]
  ): Behavior[Command] = Behaviors.receiveMessage {
    case DispatchWork(replyTo) =>
      val (taskId, newWorkQueue) = workQueue.dequeue
      val taskState = state(taskId)
      val splitCandidates = taskId & BitSet.fromSpecific(taskState.splitCandidates)
      replyTo ! CheckCandidateNode(taskId, splitCandidates, taskState.swapCandidates)
      behavior(state, newWorkQueue, pendingQueue.enqueue(taskId))

    case m =>
      context.log.info("Received message: {}", m)
      Behaviors.same
  }

  private def finished(): Behavior[Command] = {
    guardian ! LeaderGuardian.AlgorithmFinished
    Behaviors.stopped
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
