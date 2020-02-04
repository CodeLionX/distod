package com.github.codelionx.distod.types

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.BitSet


class CandidateSetSpec extends AnyWordSpec with Matchers {

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

    "have a correct equals implementation" in {
      val a = CandidateSet.from(0, 2, 3)
      val equalA = CandidateSet.from(0, 2, 3)
      val equalA2 = CandidateSet.fromBitMask(a.toBitMask)
      val b = CandidateSet(BitSet(0))

      a shouldEqual a
      a shouldEqual equalA
      a shouldEqual equalA2

      a should not equal b
    }

    "have a correct hashCode implementation" in {
      val a = CandidateSet.from(0, 2, 3)
      val equalA = CandidateSet.from(0, 2, 3)
      val equalA2 = CandidateSet.fromBitMask(a.toBitMask)
      val b = CandidateSet(BitSet(0))

      a.hashCode() shouldEqual a.hashCode()
      a.hashCode() shouldEqual equalA.hashCode()
      a.hashCode() shouldEqual equalA2.hashCode()

      a.hashCode() should not equal b.hashCode()
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

    "have a correct equals implementation" in {
      val empty = CandidateSet.empty
      val empty2 = CandidateSet.empty
      val empty3 = CandidateSet.fromBitMask(Array.emptyLongArray)
      val nonEmpty = CandidateSet.from(0)

      empty shouldEqual empty2
      empty shouldEqual empty3
      empty should not equal nonEmpty
    }

    "have a correct hashCode implementation" in {
      val empty = CandidateSet.empty
      val empty2 = CandidateSet.empty
      val empty3 = CandidateSet.fromBitMask(Array.emptyLongArray)
      val nonEmpty = CandidateSet.from(0)

      empty.hashCode() shouldEqual empty2.hashCode()
      empty.hashCode() shouldEqual empty3.hashCode()
      empty.hashCode() should not equal nonEmpty.hashCode()
    }
  }
}
