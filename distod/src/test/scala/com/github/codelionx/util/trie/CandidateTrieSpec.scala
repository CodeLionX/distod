package com.github.codelionx.util.trie

import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.{Matchers, WordSpec}


object CandidateTrieSpec {

  implicit class ExpectingCandidateSet(val cs: CandidateSet) extends AnyVal {

    def strRepr: String = s"{${cs.mkString(",")}}"

    def tuple: (CandidateSet, String) = cs -> strRepr
  }
}


class CandidateTrieSpec extends WordSpec with Matchers {

  import CandidateTrieSpec._


  "A CandidateTrie for CandidateSets" should {
    val map = CandidateTrie.empty[String]

    val csEmpty = CandidateSet.empty
    val cs0 = CandidateSet.from(0)
    val cs1 = CandidateSet.from(1)
    val cs01 = cs0 + 1
    val cs012 = cs01 + 2
    val cs013 = cs01 + 3

    "addOne" in {
      map.addOne(csEmpty.tuple)
      map.addOne(cs01.tuple)
    }

    "apply" in {
      a[NoSuchElementException] should be thrownBy {
        map(cs012)
      }
      map(cs01) shouldEqual cs01.strRepr
    }

    "get" in {
      map.get(cs0) shouldEqual None
      map.get(cs01) shouldEqual Some(cs01.strRepr)
    }

    "withPrefix" in {
      val innerTrie = map.withPrefix(cs0)
      innerTrie.get(csEmpty) shouldEqual None
      innerTrie.get(cs1) shouldEqual Some(cs01.strRepr)
    }

    "subtractOne" in {
      map.addOne(cs013.tuple)
      map.get(cs013) shouldEqual Some(cs013.strRepr)
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
      val inner = CandidateTrie.empty[String]

      noException shouldBe thrownBy {
        // trie creates missing path (nodes)
        inner.update(cs012, cs012.strRepr)
      }
      val subTrie = inner.withPrefix(cs01)
      subTrie(CandidateSet.from(2)) shouldEqual cs012.strRepr
      inner.update(cs012, "test")
      subTrie(CandidateSet.from(2)) shouldEqual "test"
    }

    "iterator" in {
      map.iterator.toSeq should contain theSameElementsInOrderAs Seq(
        csEmpty.tuple,
        cs01.tuple
      )
    }

    "map" in {
      val mapped = map.map { case (key, value) => key -> value.length }
      mapped(cs01) shouldEqual cs01.strRepr.length
    }

    "flatMap" in {
      val flatMapped = map.flatMap { case (key, value) => value.map(key -> _) }.to(CandidateTrie)
      flatMapped.iterator.toSeq should contain theSameElementsInOrderAs Seq(
        csEmpty -> '}',
        cs01 -> '}',
      )
    }

    "concat" in {
      val combined = map.concat(Seq(cs012.tuple))
      combined.toSeq should contain theSameElementsInOrderAs Seq(
        csEmpty.tuple,
        cs01.tuple,
        cs012.tuple
      )
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
}
