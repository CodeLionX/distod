package com.github.codelionx.util.largeMap.immutable

import com.github.codelionx.distod.actors.master.{CandidateState, SplitReadyCandidateState}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.largeMap.StateTestingFixtures._
import org.scalatest.{Matchers, WordSpec}


class HashMapStateSpec extends WordSpec with Matchers {

  "A HashMapState for CandidateStates" should {
    var map = HashMapState.empty[CandidateState]

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
        SplitReadyCandidateState(
          id = cs01,
          splitCandidates = CandidateSet.from(0, 1, 2),
          swapPreconditions = 0
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

    "empty" in {
      val emptyMap = HashMapState.empty[String]
      emptyMap shouldBe empty
      emptyMap.get(cs012) shouldEqual None
      emptyMap.empty shouldEqual emptyMap
    }
  }
}
