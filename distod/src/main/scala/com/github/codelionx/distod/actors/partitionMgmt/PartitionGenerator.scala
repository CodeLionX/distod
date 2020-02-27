package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, PoolRouter, Routers}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import com.github.codelionx.distod.actors.partitionMgmt.ComputePartitionProductJob.{PreviouslyComputedType, StrippedPartitionType}
import com.github.codelionx.distod.actors.partitionMgmt.PartitionManager.ProductComputed
import com.github.codelionx.distod.partitions.StrippedPartition
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.timing.Timing

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps


object PartitionGenerator {

  final case class ComputePartitions(jobs: Seq[ComputePartitionProductJob], replyTo: ActorRef[ProductComputed])

  val poolName = "partition-generator-pool"

  def name(n: Int): String = s"partition-generator-$n"

  def createPool(n: Int): PoolRouter[ComputePartitions] = Routers
    .pool(n)(
      Behaviors.supervise(apply()).onFailure[Exception](
        SupervisorStrategy.restart
          .withLoggingEnabled(true)
          .withLimit(3, 10 seconds)
      )
    )
    .withRandomRouting()
  // this leads to very slow gen-speed (looks like the pool isn't used at all and all messages are processed
  // in the head actor
//    .withRouteeProps(DispatcherSelector.fromConfig("distod.cpu-bound-tasks-dispatcher"))

  private def apply(): Behavior[ComputePartitions] = Behaviors.setup(context =>
    new PartitionGenerator(context).start()
  )
}


class PartitionGenerator(context: ActorContext[PartitionGenerator.ComputePartitions]) {

  import PartitionGenerator._


  private val timing: Timing = Timing(context.system)

  def start(): Behavior[ComputePartitions] = Behaviors.receiveMessage {
    case ComputePartitions(jobs, replyTo) =>
      timing.unsafeTime("Partition generation") {
        computePartitionChain { case (key, newPartition) =>
          replyTo ! ProductComputed(key, newPartition)
        }(Map.empty, jobs)
        Behaviors.same
      }
  }

  private def computePartitionChain(
      onNewPartition: (CandidateSet, StrippedPartition) => Unit
  ): (Map[CandidateSet, StrippedPartition], Seq[ComputePartitionProductJob]) => Unit = {
    @tailrec
    def loop(partitions: Map[CandidateSet, StrippedPartition], remainingJobs: Seq[ComputePartitionProductJob]): Unit = {
      val job :: newRemainingJobs = remainingJobs
      // only compute product if we did not already compute this partition
      val newPartitions = if(!partitions.contains(job.key)) {
        job match {
          case ComputeFromPredecessorsProduct(key, partitionA, partitionB, store) =>
            val pA = partitionA match {
              case StrippedPartitionType(p) => p
              case PreviouslyComputedType(candidate) => partitions(candidate)
            }
            val pB = partitionB match {
              case StrippedPartitionType(p) => p
              case PreviouslyComputedType(candidate) => partitions(candidate)
            }
            val newPartition = pA * pB
            if (store) onNewPartition(key, newPartition)
            partitions + (key -> newPartition)

          case ComputeFromSingletonsProduct(key, singletonPartitions, store) =>
            val newPartition = singletonPartitions.map(_.stripped).reduceLeft(_ * _)
            if (store) onNewPartition(key, newPartition)
            partitions + (key -> newPartition)

        }
      } else {
        partitions
      }
      if (newRemainingJobs != Nil)
        loop(newPartitions, newRemainingJobs)
    }

    loop
  }

}