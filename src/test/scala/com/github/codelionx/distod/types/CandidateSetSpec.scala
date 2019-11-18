package com.github.codelionx.distod.types

import org.scalatest.{Matchers, WordSpec}

class CandidateSetSpec extends WordSpec with Matchers {

  "A candidate set" should {
    "calculate its predecessors correctly" in {
      val parent = CandidateSet(1, 2, 3)
      val expected = Seq(CandidateSet(1, 2), CandidateSet(2, 3), CandidateSet(1, 3))

      val preds = parent.predecessors
      preds should contain theSameElementsAs expected
    }
  }

  "An empty candidate set" should {
    "have no predecessors" in {
      val parent = CandidateSet.empty

      val preds = parent.predecessors
      preds shouldBe empty
    }
  }
}
