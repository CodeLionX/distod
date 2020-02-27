package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.Settings.PartitionCompactionSettings
import com.github.codelionx.distod.actors.partitionMgmt.ComputePartitionProductJob.{PreviouslyComputedType, StrippedPartitionType}
import com.github.codelionx.distod.actors.partitionMgmt.JobChainer.storeDepth
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
      val store = (l: Int) => l >= parent.size - JobChainer.storeDepth + 1

      val chain = JobChainer.calcJobChain(parent, partitions)
      chain.size shouldBe 6
      chain shouldEqual Seq(
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1), StrippedPartitionType(emptyPartition), StrippedPartitionType(emptyPartition), store(2)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 2), StrippedPartitionType(emptyPartition), StrippedPartitionType(emptyPartition), store(2)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1, 2), PreviouslyComputedType(CandidateSet.from(0, 1)), PreviouslyComputedType(CandidateSet.from(0, 2)), store(3)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 3), StrippedPartitionType(emptyPartition), StrippedPartitionType(emptyPartition), store(2)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1, 3), PreviouslyComputedType(CandidateSet.from(0, 1)), PreviouslyComputedType(CandidateSet.from(0, 3)), store(3)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1, 2, 3), PreviouslyComputedType(CandidateSet.from(0, 1, 2)), PreviouslyComputedType(CandidateSet.from(0, 1, 3)), store(4))
      )
    }

    "compute from singleton partitions" in {
      val partitions = CompactingPartitionMap(compactionSettings).from(singletonPartitions)
      val parent = CandidateSet.from(0, 1, 2, 3)
      val expectedPartitions = parent.toSeq.map(i => singletonPartitions(CandidateSet.from(i)))

      val chain = JobChainer.calcLightJobChain(parent, partitions)
      chain.size shouldBe 1
      chain shouldEqual Seq(
        ComputeFromSingletonsProduct(parent, expectedPartitions, store = true)
      )
    }

    "compute a minimal job chain if partitions are present" in {
      val partitions = CompactingPartitionMap(compactionSettings).from(singletonPartitions)
      partitions + (CandidateSet.from(0, 1, 2), emptyPartition)
      partitions + (CandidateSet.from(0, 1, 2, 3), emptyPartition)
      val parent = CandidateSet.from(0, 1, 2, 3, 4)
      val store = (l: Int) => l >= parent.size - JobChainer.storeDepth + 1

      val chain = JobChainer.calcJobChain(parent, partitions)
      chain.size shouldBe 5
      chain shouldEqual Seq(
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1), StrippedPartitionType(emptyPartition), StrippedPartitionType(emptyPartition), store(2)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 4), StrippedPartitionType(emptyPartition), StrippedPartitionType(emptyPartition), store(2)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1, 4), PreviouslyComputedType(CandidateSet.from(0, 1)), PreviouslyComputedType(CandidateSet.from(0, 4)), store(3)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1, 2, 4), StrippedPartitionType(emptyPartition), PreviouslyComputedType(CandidateSet.from(0, 1, 4)), store(4)),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1, 2, 3, 4), StrippedPartitionType(emptyPartition), PreviouslyComputedType(CandidateSet.from(0, 1, 2, 4)), store(5)),
      )
    }


    "compute a minimal job chain if a partition and singleton partitions are present" in {
      val partitions = CompactingPartitionMap(compactionSettings).from(singletonPartitions)
      partitions + (CandidateSet.from(0, 1, 2, 3), emptyPartition)
      val parent = CandidateSet.from(0, 1, 2, 3, 4)
      val storePredecessor = storeDepth > 1
      val expectedPartitions = Seq(0, 1, 2, 3).map(i => singletonPartitions(CandidateSet.from(i)))

      val chain = JobChainer.calcLightJobChain(parent, partitions)
      chain.size shouldBe 2
      chain shouldEqual Seq(
        ComputeFromSingletonsProduct(CandidateSet.from(0, 1, 2, 4), expectedPartitions, storePredecessor),
        ComputeFromPredecessorsProduct(CandidateSet.from(0, 1, 2, 3, 4), StrippedPartitionType(emptyPartition), PreviouslyComputedType(CandidateSet.from(0, 1, 2, 4)), store = true)
      )
    }
  }
}
