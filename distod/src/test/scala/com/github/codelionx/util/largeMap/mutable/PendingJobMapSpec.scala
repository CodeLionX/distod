package com.github.codelionx.util.largeMap.mutable

import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class PendingJobMapSpec extends AnyWordSpec with Matchers {

  type SpecificPendingJobMap = PendingJobMap[CandidateSet, Int]

  "An empty pending job map" should {
    val empty: SpecificPendingJobMap = PendingJobMap.empty
    val key: CandidateSet = CandidateSet.from(0)

    "throw an exception when accessed with apply" in {
      an[Exception] should be thrownBy empty(key)
    }

    "return none when accessed with get" in {
      val result = empty.get(key)
      result shouldBe None
    }

    "not change when a key is removed" in {
      empty.removeKey(key)
      empty shouldEqual empty
    }

    "not change when a pair is removed" in {
      empty - (key -> 0)
      empty shouldEqual empty
    }

    "allow adding a new pair" in {
      val map: SpecificPendingJobMap = PendingJobMap.empty
      map + (key -> 1)
      map shouldEqual PendingJobMap(key -> Seq(1))
    }

    "merge with other iterable" in {
      val map: SpecificPendingJobMap = PendingJobMap.empty
      map ++ Seq(key -> Seq(0))
      map shouldEqual PendingJobMap(key -> Seq(0))
    }

    "not contain anything" in {
      empty.contains(key) shouldBe false
    }

    "print an empty string" in {
      empty.toString() shouldEqual "PendingJobMap()"
      empty.mkString(",") shouldEqual ""
    }
  }

  "An filled pending job map" should {
    val newKey: CandidateSet = CandidateSet.from(0)
    val existingKey: CandidateSet = CandidateSet.from(5)
    val existingKeyValues: Seq[Int] = Seq(9)
    val existingMapping: (CandidateSet, Seq[Int]) = CandidateSet.from(7, 12, 5) -> Seq(5, 3, 9)
    val values: Seq[Int] = Seq(13, 4)

    val pendingJobMap: SpecificPendingJobMap = PendingJobMap(existingMapping, existingKey -> existingKeyValues)

    "return the values for an existing key (apply)" in {
      pendingJobMap(existingKey) shouldEqual existingKeyValues
    }

    "throw an exception when missing key is accessed with apply" in {
      an[Exception] should be thrownBy pendingJobMap(newKey)
    }

    "return the values for an existing key (get)" in {
      pendingJobMap.get(existingKey) shouldEqual Some(existingKeyValues)
    }

    "return none when missing key is accessed with get" in {
      val result = pendingJobMap.get(newKey)
      result shouldBe None
    }

    "remove a key value mapping" in {
      val newMap = pendingJobMap.clone()
      newMap.removeKey(existingKey)
      newMap shouldEqual PendingJobMap(existingMapping)
    }

    "remove a single value" in {
      val newMap = pendingJobMap.clone()
      newMap  - (existingKey -> 9)
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> Seq.empty)
    }

    "add a new value to existing key" in {
      val newMap = pendingJobMap.clone()
      newMap + (existingKey -> 1)
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> existingKeyValues.appendedAll(Seq(1)))
    }

    "add a new key and value" in {
      val newMap = pendingJobMap.clone()
      newMap + (newKey -> 5)
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> existingKeyValues, newKey -> Seq(5))
    }

    "merge with other iterable" in {
      val newMap = pendingJobMap.clone()
      newMap ++ Seq(newKey -> Seq(0))
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> existingKeyValues, newKey -> Seq(0))
    }

    "merge with other pending job map" in {
      val otherMappings: Seq[(CandidateSet, Seq[Int])] = Seq(
        existingKey -> values,
        CandidateSet.from(0, 1) -> Seq(12, 578)
      )
      val otherMap: PendingJobMap[CandidateSet, Int] = PendingJobMap.from(otherMappings)
      val newMap = pendingJobMap.clone()
      newMap ++ otherMap
      newMap shouldEqual PendingJobMap(
        existingMapping,
        existingKey -> existingKeyValues.appendedAll(values),
        otherMappings(1)
      )
    }

    "contain an existing key" in {
      pendingJobMap.contains(existingKey) shouldBe true
    }

    "not contain an new key" in {
      pendingJobMap.contains(newKey) shouldBe false
    }
  }
}
