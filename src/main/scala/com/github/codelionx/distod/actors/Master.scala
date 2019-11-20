package com.github.codelionx.distod.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.actors.Master.{Command, LocalPeers}
import com.github.codelionx.distod.partitions.StrippedPartition
import com.github.codelionx.distod.protocols.DataLoadingProtocol._
import com.github.codelionx.distod.protocols.PartitionManagementProtocol
import com.github.codelionx.distod.protocols.PartitionManagementProtocol._
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}


object Master {

  sealed trait Command
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command
  private final case class WrappedPartitionEvent(e: PartitionManagementProtocol.PartitionEvent) extends Command

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
    val loadingEventMapper = context.messageAdapter(e => WrappedLoadingEvent(e))
    val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))

    dataReader ! LoadPartitions(loadingEventMapper)

    def onLoadingEvent(loadingEvent: DataLoadingEvent): Behavior[Command] = loadingEvent match {
      case PartitionsLoaded(t@PartitionedTable(name, headers, partitions)) =>
        context.log.info("Finished loading dataset {}", name)
        println(s"Table headers: ${headers.mkString(",")}")
        println(partitions.map(p =>
          s"Partition(numberClasses=${p.numberClasses},numberElements=${p.numberElements})"
        ).mkString("\n"))

        // stop data reader to free up resources
        dataReader ! Stop

        // generate first level candidates
        val L1candidates = generateFirstLevelCandidates(t.m)

        // insert empty partition
        partitionManager ! InsertPartition(CandidateSet.empty, StrippedPartition(
          numberElements = t.n,
          numberClasses = 1,
          equivClasses = IndexedSeq((0 until t.n).toSet)
        ))
        // insert level1 partitions
        L1candidates.zipWithIndex.foreach { case (candidate, index) =>
          partitionManager ! InsertPartition(candidate, partitions(index))

        }

        // ask for advanced partitions as a test
        partitionManager ! LookupStrippedPartition(CandidateSet(0, 1, 2), partitionEventMapper)

        Behaviors.same
    }

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

    Behaviors
      .receiveMessage[Command] {
        case WrappedLoadingEvent(event) => onLoadingEvent(event)
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


  private def generateFirstLevelCandidates(nAttributes: Int): IndexedSeq[CandidateSet] =
    (0 until nAttributes).map(columnId => CandidateSet(columnId))
}
