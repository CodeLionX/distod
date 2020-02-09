package com.github.codelionx.distod.actors.partitionMgmt

import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector, SupervisorStrategy}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, PoolRouter, Routers}
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
    .withRoundRobinRouting()
    .withRouteeProps(DispatcherSelector.fromConfig("distod.cpu-bound-tasks-dispatcher"))

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
        computePartition { case (key, newPartition) =>
          replyTo ! ProductComputed(key, newPartition)
        }(Map.empty, jobs)
        Behaviors.same
      }
  }

  private def computePartition(
      onNewPartition: (CandidateSet, StrippedPartition) => Unit
  ): (Map[CandidateSet, StrippedPartition], Seq[ComputePartitionProductJob]) => Unit = {
    @tailrec
    def loop(partitions: Map[CandidateSet, StrippedPartition], remainingJobs: Seq[ComputePartitionProductJob]): Unit = {
      val job :: newRemainingJobs = remainingJobs
      val pA = job.partitionA match {
        case Right(p) => p
        case Left(candidate) => partitions(candidate)
      }
      val pB = job.partitionB match {
        case Right(p) => p
        case Left(candidate) => partitions(candidate)
      }
      val newPartition = (pA * pB).asInstanceOf[StrippedPartition]
      if(job.store)
        onNewPartition(job.key, newPartition)

      if (newRemainingJobs != Nil)
        loop(partitions + (job.key -> newPartition), newRemainingJobs)
    }

    loop
  }

}