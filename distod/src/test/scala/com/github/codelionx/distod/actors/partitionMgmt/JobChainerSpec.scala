package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.Settings.PartitionCompactionSettings
import com.github.codelionx.distod.partitions.FullPartition
import com.github.codelionx.distod.types.{CandidateSet, TupleValueMap}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps


class JobChainerSpec extends AnyWordSpec with Matchers {

  private val compactionSettings = new PartitionCompactionSettings {
    override def enabled: Boolean = false

    override def interval: FiniteDuration = 1 minute
  }

  "The JobChainer" should {

    val emptyFullPartition = FullPartition(0, 0, 0, Array.empty, TupleValueMap.empty)
    val emptyPartition = emptyFullPartition.stripped
    val singletonPartitions = (0 to 10).map(id => CandidateSet.from(id) -> emptyFullPartition).toMap

    "compute a minimal job chain from scratch" in {
      val partitions = CompactingPartitionMap(compactionSettings).from(singletonPartitions)
      val parent = CandidateSet.from(0, 1, 2, 3)

      val chain = JobChainer.calcJobChain(parent, partitions)
      chain.size shouldBe 6
      chain shouldEqual Seq(
        ComputePartitionProductJob(CandidateSet.from(0, 1), Right(emptyPartition), Right(emptyPartition), store = false),
        ComputePartitionProductJob(CandidateSet.from(0, 2), Right(emptyPartition), Right(emptyPartition), store = false),
        ComputePartitionProductJob(CandidateSet.from(0, 1, 2), Left(CandidateSet.from(0, 1)), Left(CandidateSet.from(0, 2)), store = true),
        ComputePartitionProductJob(CandidateSet.from(0, 3), Right(emptyPartition), Right(emptyPartition), store = false),
        ComputePartitionProductJob(CandidateSet.from(0, 1, 3), Left(CandidateSet.from(0, 1)), Left(CandidateSet.from(0, 3)), store = true),
        ComputePartitionProductJob(CandidateSet.from(0, 1, 2, 3), Left(CandidateSet.from(0, 1, 2)), Left(CandidateSet.from(0, 1, 3)), store = true)
      )
    }

    "compute a minimal job chain if partitions are present" in {
      val partitions = CompactingPartitionMap(compactionSettings).from(singletonPartitions)
      partitions + (CandidateSet.from(0, 1, 2), emptyPartition)
      partitions + (CandidateSet.from(0, 1, 2, 3), emptyPartition)
      val parent = CandidateSet.from(0, 1, 2, 3, 4)

      val chain = JobChainer.calcJobChain(parent, partitions)
      chain.size shouldBe 5
      chain shouldEqual Seq(
        ComputePartitionProductJob(CandidateSet.from(0, 1), Right(emptyPartition), Right(emptyPartition), store = false),
        ComputePartitionProductJob(CandidateSet.from(0, 4), Right(emptyPartition), Right(emptyPartition), store = false),
        ComputePartitionProductJob(CandidateSet.from(0, 1, 4), Left(CandidateSet.from(0, 1)), Left(CandidateSet.from(0, 4)), store = false),
        ComputePartitionProductJob(CandidateSet.from(0, 1, 2, 4), Right(emptyPartition), Left(CandidateSet.from(0, 1, 4)), store = true),
        ComputePartitionProductJob(CandidateSet.from(0, 1, 2, 3, 4), Right(emptyPartition), Left(CandidateSet.from(0, 1, 2, 4)), store = true),
      )
    }
  }
}
