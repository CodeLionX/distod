package com.github.codelionx.distod.actors.worker

import com.github.codelionx.distod.partitions.{Partition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.{ConstantOrderDependency, EquivalencyOrderDependency}
import org.scalatest.{Matchers, WordSpec}


class CandidateValidationSpec extends WordSpec with Matchers {

  "The candidate validation trait" should {
    val tester = new CandidateValidation {}

    val candidateId = CandidateSet.from(2, 3)
    val attributes = 0 until 4
    val partitionCol0 = Partition.fullFrom(Array("a", "b", "a", "b"))
    val partitionCol1 = Partition.fullFrom(Array("a", "b", "c", "c"))
    val partitionCol2 = Partition.fullFrom(Array("a", "a", "a", "d"))
    val partitionCol3 = Partition.fullFrom(Array("a", "a", "c", "e"))

    "find constant order dependencies correctly" in {
      // {D}: [] -> C
      val splitCandidates = candidateId
      val errors = Map(
        candidateId -> (partitionCol2.stripped * partitionCol3.stripped).error,
        CandidateSet.from(2) -> partitionCol2.stripped.error,
        CandidateSet.from(3) -> partitionCol3.stripped.error
      )

      val result = tester.checkSplitCandidates(candidateId, splitCandidates, attributes, errors)
      result.validOds shouldEqual Seq(ConstantOrderDependency(CandidateSet.from(3), 2))
      result.removedCandidates shouldEqual CandidateSet.from(0, 1, 2)
    }

    "find regular equivalency order dependencies correctly" in {
      // {}: C ~ D
      val swapCandidates = Seq((2, 3))
      val singletonPartitions = Map(
        CandidateSet.from(2) -> partitionCol2,
        CandidateSet.from(3) -> partitionCol3
      )
      val candidatePartitions = Map(
        CandidateSet.empty -> StrippedPartition(
          numberElements = attributes.size,
          numberClasses = 1,
          equivClasses = IndexedSeq(attributes.toSet)
        )
      )
      val result = tester.checkSwapCandidates(candidateId, swapCandidates, singletonPartitions, candidatePartitions)

      result.validOds shouldEqual Seq(EquivalencyOrderDependency(CandidateSet.empty, 2, 3))
      result.removedCandidates shouldEqual swapCandidates
    }

    "find reverse equivalency order dependencies correctly" in {
      // {C}: A ~ rev. D
      val candidate = CandidateSet.from(0, 2, 3)
      val swapCandidates = Seq((0, 3))
      val singletonPartitions = Map(
        CandidateSet.from(0) -> partitionCol0,
        CandidateSet.from(3) -> partitionCol3
      )
      val candidatePartitions = Map(
        CandidateSet.from(0) -> partitionCol0.stripped,
        CandidateSet.from(2) -> partitionCol2.stripped,
        CandidateSet.from(3) -> partitionCol3.stripped,
      )
      val result = tester.checkSwapCandidates(candidate, swapCandidates, singletonPartitions, candidatePartitions)

      result.validOds shouldEqual Seq(EquivalencyOrderDependency(CandidateSet.from(2), 0, 3, reverse = true))
      result.removedCandidates shouldEqual swapCandidates
    }

    "pass the bug guard: duplicate result" in {
      // {}: B ~ C
      val candidate = CandidateSet.from(1, 2)
      val swapCandidates = Seq((1, 2))
      val singletonPartitions = Map(
        CandidateSet.from(1) -> partitionCol1,
        CandidateSet.from(2) -> partitionCol2
      )
      val candidatePartitions = Map(
        CandidateSet.empty -> StrippedPartition(
          numberElements = attributes.size,
          numberClasses = 1,
          equivClasses = IndexedSeq(attributes.toSet)
        )
      )
      val result = tester.checkSwapCandidates(candidate, swapCandidates, singletonPartitions, candidatePartitions)
      println(result)

      result.validOds shouldEqual Seq(EquivalencyOrderDependency(CandidateSet.empty, 1, 2))
      result.removedCandidates shouldEqual swapCandidates
    }
  }
}
