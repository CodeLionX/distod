package com.github.codelionx.util.largeMap.mutable

import com.github.codelionx.distod.actors.master.CandidateState
import com.github.codelionx.distod.actors.master.CandidateState.NewSplitCandidates
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.util.largeMap.StateTestingFixtures._
import org.scalatest.{Matchers, WordSpec}


class CandidateTrieSpec extends WordSpec with Matchers {

  "A CandidateTrie for CandidateStates" should {
    val map = CandidateTrie.empty[CandidateState]

    "addOne" in {
      map.addOne(csEmpty.tuple)
      map.addOne(cs01.tuple)
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

    "subtractOne" in {
      map.addOne(cs013.tuple)
      map.get(cs013) shouldEqual Some(cs013.stateRepr)
      noException shouldBe thrownBy {
        map.subtractOne(cs013)
      }
      map.get(cs013) shouldEqual None
    }

    "contains" in {
      map.contains(cs01) shouldBe true
      map.contains(cs012) shouldBe false
    }

    "update" in {
      val inner = FastutilState.empty[CandidateState]

      noException shouldBe thrownBy {
        inner.update(cs012, cs012.stateRepr)
      }

      inner(CandidateSet.from(0, 1, 2)) shouldEqual cs012.stateRepr
      inner.update(cs012, CandidateState(csEmpty))
      inner(CandidateSet.from(0, 1, 2)) shouldEqual CandidateState(csEmpty)
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
      val inner = CandidateTrie(cs01 -> CandidateState(cs01).updated(
        NewSplitCandidates(CandidateSet.from(0, 1, 2))
      ))
      val flatMapped = inner.flatMap { case (_, value) => value.splitCandidates }
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

    "updateWith" in {
      // do not change
      map.updateWith(cs012)(_ => None)
      map.toSeq should contain theSameElementsInOrderAs Seq(csEmpty.tuple, cs01.tuple)
      // create new node
      map.updateWith(cs012)(_ => Some(CandidateState(csEmpty)))
      map.toSeq should contain theSameElementsInOrderAs Seq(csEmpty.tuple, cs01.tuple, cs012 -> CandidateState(csEmpty))
      // update existing node
      map.updateWith(cs012)(_ => Some(cs012.stateRepr))
      map.toSeq should contain theSameElementsInOrderAs Seq(csEmpty.tuple, cs01.tuple, cs012.tuple)
    }

    "clear" in {
      noException shouldBe thrownBy {
        map.clear()
      }
      map shouldBe empty
    }

    "empty" in {
      val emptyTrie = CandidateTrie.empty[String]
      emptyTrie shouldBe empty
      emptyTrie.get(cs012) shouldEqual None
    }
  }

  "A CandidateTrie specifically" should {

    "withPrefix" in {
      val map = CandidateTrie(cs01.tuple)

      val innerTrie = map.withPrefix(cs0)
      innerTrie.get(csEmpty) shouldEqual None
      innerTrie.get(cs1) shouldEqual Some(cs01.stateRepr)
    }

    "update" in {
      val inner = CandidateTrie.empty[CandidateState]

      noException shouldBe thrownBy {
        // trie creates missing path (nodes)
        inner.update(cs012, cs012.stateRepr)
      }
      val subTrie = inner.withPrefix(cs01)
      subTrie(CandidateSet.from(2)) shouldEqual cs012.stateRepr
      inner.update(cs012, CandidateState(csEmpty))
      subTrie(CandidateSet.from(2)) shouldEqual CandidateState(csEmpty)
    }

    "updateIfDefinedWith" in {
      val m = CandidateTrie(cs01.tuple)
      // do not change if value not defined
      m.updateIfDefinedWith(cs0)(_ => CandidateState(csEmpty))
      m.toSeq should contain theSameElementsInOrderAs Seq(cs01.tuple)
      // change value if value defined
      m.updateIfDefinedWith(cs01)(_ => CandidateState(csEmpty))
      m.toSeq should contain theSameElementsInOrderAs Seq(cs01 -> CandidateState(csEmpty))
    }
  }
}
