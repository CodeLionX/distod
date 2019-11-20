package com.github.codelionx.distod.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{Behavior, Terminated}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.partitions.StrippedPartition
import com.github.codelionx.distod.protocols.DataLoadingProtocol.{DataLoaded, DataLoadingEvent}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.LookupStrippedPartition
import com.github.codelionx.distod.types.{CandidateSet, PartitionedTable}

import scala.collection.immutable.BitSet


object LeaderGuardian {

  sealed trait Command
  private final case class WrappedLoadingEvent(dataLoadingEvent: DataLoadingEvent) extends Command
  private final case class WrappedPartitionEvent(e: PartitionManagementProtocol.PartitionEvent) extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    val loadingEventMapper = context.messageAdapter(e => WrappedLoadingEvent(e))
    val partitionEventMapper = context.messageAdapter(e => WrappedPartitionEvent(e))

    context.log.info("LeaderGuardian started, spawning actors ...")

    val clusterTester = context.spawn[Nothing](ClusterTester(), ClusterTester.name)
    val dataReader = context.spawn(DataReader(loadingEventMapper), DataReader.name)

    val partitionManager = context.spawn(PartitionManager(), PartitionManager.name)
    context.watch(partitionManager)

    context.log.info("actors started, waiting for data")

    // testCPUhogging(context)

    def onLoadingEvent(loadingEvent: DataLoadingEvent): Behavior[Command] = loadingEvent match {
      case DataLoaded(t@PartitionedTable(name, headers, partitions)) =>
        context.log.info("Finished loading dataset {}", name)
        println(s"Table headers: ${headers.mkString(",")}")
        println(partitions.map(p =>
          s"Partition(numberClasses=${p.numberClasses},numberElements=${p.numberElements})"
        ).mkString("\n"))

        // insert empty partition
        partitionManager ! PartitionManagementProtocol.InsertPartition(CandidateSet.empty, StrippedPartition(
          numberElements = t.n,
          numberClasses = 1,
          equivClasses = IndexedSeq((0 until t.n).toSet)
        ))
        // insert level1 partitions
        (0 until t.m).foreach(columnId =>
          partitionManager ! PartitionManagementProtocol.InsertPartition(CandidateSet(BitSet(columnId)), partitions(columnId))
        )

        // ask for advanced partitions
        partitionManager ! LookupStrippedPartition(CandidateSet(1, 2), partitionEventMapper)
        partitionManager ! LookupStrippedPartition(CandidateSet(0, 1, 2), partitionEventMapper)
        partitionManager ! LookupStrippedPartition(CandidateSet(1, 2), partitionEventMapper)
        partitionManager ! LookupStrippedPartition(CandidateSet(1, 2, 3), partitionEventMapper)
        Behaviors.same
    }

    def onPartitionEvent(event: PartitionManagementProtocol.PartitionEvent): Behavior[Command] = event match {
      case PartitionManagementProtocol.PartitionFound(key, value) =>
        println("Received partition", key, value)
        Behaviors.same
      case PartitionManagementProtocol.StrippedPartitionFound(key, value) =>
        println("Received stripped partition", key, value)
        Behaviors.same
    }

    Behaviors
      .receiveMessage[Command] {
        case WrappedLoadingEvent(event) => onLoadingEvent(event)
        case WrappedPartitionEvent(event) => onPartitionEvent(event)
      }
      .receiveSignal {
        case (context, Terminated(ref)) =>
          context.log.info(s"$ref has stopped working!")
          Behaviors.stopped
      }
  }

  def testCPUhogging(context: ActorContext[Command]): Unit = {
    val settings = Settings(context.system)
    val n = 16
    context.log.info("Starting {} hoggers", n)
    (0 until n).foreach { i =>
      val ref = context.spawnAnonymous(CPUHogger(i), settings.cpuBoundTaskDispatcher)
      context.watch(ref)
    }
  }
}
