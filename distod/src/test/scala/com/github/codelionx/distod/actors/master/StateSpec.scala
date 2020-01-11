package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.CandidateState.NewSplitCandidates
import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.{Matchers, WordSpec}


object StateSpec {

  implicit class ExpectingCandidateSet(val cs: CandidateSet) extends AnyVal {

    def stateRepr: CandidateState = CandidateState(cs)

    def tuple: (CandidateSet, CandidateState) = cs -> stateRepr
  }
}


class StateSpec extends WordSpec with Matchers {

  import StateSpec._


  "A State for CandidateStates" should {
    var map = State.empty[CandidateState]

    val csEmpty = CandidateSet.empty
    val cs0 = CandidateSet.from(0)
    val cs1 = CandidateSet.from(1)
    val cs01 = cs0 + 1
    val cs012 = cs01 + 2
    val cs013 = cs01 + 3

    "updated (addition)" in {
      map += csEmpty.tuple
      map += cs01.tuple
    }

    "apply" in {
      a[NoSuchElementException] should be thrownBy {
        map(cs012)
      }
      map(cs01) shouldEqual cs01.stateRepr
    }

    "get" in {
      map.get(cs0) shouldEqual None
      map.get(cs01) shouldEqual Some(cs01.stateRepr)
    }

    "removed (subtraction)" in {
      map += cs013.tuple
      map.get(cs013) shouldEqual Some(cs013.stateRepr)
      noException shouldBe thrownBy {
        map = map.removed(cs013)
      }
      map.get(cs013) shouldEqual None
    }

    "contains" in {
      map.contains(cs01) shouldBe true
      map.contains(cs012) shouldBe false
    }

    "iterator" in {
      map.iterator.toSeq should contain theSameElementsInOrderAs Seq(
        csEmpty.tuple,
        cs01.tuple
      )
    }

    "map" in {
      val mapped = map.map { case (key, value) => key -> value.splitChecked }
      mapped(cs01) shouldEqual cs01.stateRepr.splitChecked
    }

    "flatMap" in {
      val updatedMap = map.updated(
        cs01,
        CandidateState(cs01).updated(
          NewSplitCandidates(CandidateSet.from(0, 1, 2))
        )
      )
      val flatMapped = updatedMap.flatMap { case (_, value) => value.splitCandidates }
      flatMapped.iterator.toSeq should contain theSameElementsInOrderAs Seq(0, 1, 2)
    }

    "concat" in {
      val combined = map.concat(Seq(cs012.tuple))
      combined.toSeq should contain theSameElementsInOrderAs Seq(
        csEmpty.tuple,
        cs01.tuple,
        cs012.tuple
      )
    }

    "updatedWith" in {
      val notChanged = map.updatedWith(cs012)(_ => None)
      notChanged.toSeq should contain theSameElementsInOrderAs Seq(csEmpty.tuple, cs01.tuple)

      val newEntry = map.updatedWith(cs012)(_ => Some(CandidateState(csEmpty)))
      newEntry.toSeq should contain theSameElementsInOrderAs Seq(csEmpty.tuple, cs01.tuple, cs012 -> CandidateState(csEmpty))

      val updatedExisting = map.updatedWith(cs012)(_ => Some(cs012.stateRepr))
      updatedExisting.toSeq should contain theSameElementsInOrderAs Seq(csEmpty.tuple, cs01.tuple, cs012.tuple)
    }

//    "updateIfDefinedWith" in {
//      val m = CandidateTrie(cs01.tuple)
//      // do not change if value not defined
//      m.updateIfDefinedWith(cs0)(_ => "test")
//      m.toSeq should contain theSameElementsInOrderAs Seq(cs01.tuple)
//      // change value if value defined
//      m.updateIfDefinedWith(cs01)(_ => "test")
//      m.toSeq should contain theSameElementsInOrderAs Seq(cs01 -> "test")
//    }
//
//    "clear" in {
//      noException shouldBe thrownBy {
//        map.clear()
//      }
//      map shouldBe empty
//    }

    "empty" in {
      val emptyMap = State.empty[String]
      emptyMap shouldBe empty
      emptyMap.get(cs012) shouldEqual None
      emptyMap.empty shouldEqual emptyMap
    }
  }
}
