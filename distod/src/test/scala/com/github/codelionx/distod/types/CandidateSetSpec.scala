package com.github.codelionx.distod.types

import org.scalatest.{Matchers, WordSpec}


class CandidateSetSpec extends WordSpec with Matchers {

  private val attributes = Set(0, 1, 2, 3)

  "A candidate set" should {
    "calculate its predecessors correctly" in {
      val parent = CandidateSet.from(1, 2, 3)
      val expected = Set(CandidateSet.from(1, 2), CandidateSet.from(2, 3), CandidateSet.from(1, 3))

      val preds = parent.predecessors
      preds should contain theSameElementsAs expected
    }

    "calculate its successors correctly" in {
      val parent = CandidateSet.from(1, 2)
      val expected = Set(CandidateSet.from(0, 1, 2), CandidateSet.from(1, 2, 3))

      val succs = parent.successors(attributes)
      succs should contain theSameElementsAs expected
    }
  }

  "An empty candidate set" should {
    "have no predecessors" in {
      val parent = CandidateSet.empty

      val preds = parent.predecessors
      preds shouldBe empty
    }

    "have singleton set successors" in {
      val parent = CandidateSet.empty
      val succs = parent.successors(attributes)
      succs shouldEqual attributes.map(CandidateSet.from(_))
    }
  }
}
