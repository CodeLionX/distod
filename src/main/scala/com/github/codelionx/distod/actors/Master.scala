package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.actors.Master.{Command, LocalPeers}
import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.protocols.DataLoadingProtocol._
import com.github.codelionx.distod.protocols.PartitionManagementProtocol
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}


object Master {

  sealed trait Command
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command
  private final case class WrappedPartitionEvent(e: PartitionManagementProtocol.PartitionEvent) extends Command

  val MasterServiceKey: ServiceKey[Command] = ServiceKey("master")

  val name = "master"

  def apply(
      guardian: ActorRef[LeaderGuardian.Command],
      dataReader: ActorRef[DataLoadingCommand],
      partitionManager: ActorRef[PartitionCommand]
  ): Behavior[Command] = Behaviors.setup(context =>
    new Master(context, LocalPeers(guardian, dataReader, partitionManager)).start()
  )

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


class Master(context: ActorContext[Command], localPeers: LocalPeers) {

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
      case PartitionsLoaded(t @ PartitionedTable(name, headers, partitions)) =>
        context.log.info("Finished loading dataset {} with headers: {}", name, headers.mkString(","))

        // stop data reader to free up resources
        dataReader ! Stop

        val attributes = 0 until t.nAttributes

        // L0: root candidate node
        val rootCandidateState = generateLevel0(attributes, t.nTuples)

        // L1: single attribute candidate nodes
        val L1candidateState = generateLevel1(attributes, partitions)

        testPartitionMgmt()
//        behavior(rootCandidateState ++ L1candidateState)
    }

    Behaviors.receiveMessagePartial[Command] {
      case WrappedLoadingEvent(event) => onLoadingEvent(event)
    }
  }

  private def testPartitionMgmt(): Behavior[Command] = {
    val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))

    // ask for advanced partitions as a test
    partitionManager ! LookupStrippedPartition(CandidateSet(0, 1, 2), partitionEventMapper)

    def onPartitionEvent(event: PartitionEvent): Behavior[Command] = event match {
      case PartitionFound(key, value) =>
        println("Received partition", key, value)
        Behaviors.same
      case StrippedPartitionFound(key, value) =>
        println("Received stripped partition", key, value)
        // Behaviors.same

        // FINISHED for now
        finished()
    }

    Behaviors.receiveMessagePartial {
      case WrappedPartitionEvent(event) => onPartitionEvent(event)
    }
  }

  private def behavior(state: Map[CandidateSet, CandidateState]): Behavior[Command] = Behaviors.receiveMessage {
    case _ => ???
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
    val L1candidates = attributes.map(columnId => CandidateSet(columnId))
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
