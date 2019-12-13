package com.github.codelionx.distod.types

import org.scalatest.{Matchers, WordSpec}


class PendingJobMapSpec extends WordSpec with Matchers {

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
      val newMap = empty.keyRemoved(key)
      newMap shouldEqual empty
    }

    "not change when a pair is removed" in {
      val newMap = empty - (key -> Seq.empty)
      newMap shouldEqual empty
    }

    "allow adding a new pair (of other value type)" in {
      val newMap = empty + (key -> .1)
      newMap shouldEqual PendingJobMap(key -> Seq(.1))
    }

    "merge with other iterable" in {
      val newMap = empty ++ Seq(key -> Seq(0))
      newMap shouldEqual PendingJobMap(key -> Seq(0))
    }

    "merge with other pending job map (of other value type)" in {
      val otherMap: PendingJobMap[CandidateSet, Double] = PendingJobMap(CandidateSet.from(0, 1) -> Seq(12.34, 56.78))
      val newMap = empty ++ otherMap
      newMap shouldEqual otherMap
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
    val values: Seq[Double] = Seq(13, .4)

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
      val newMap = pendingJobMap.keyRemoved(existingKey)
      newMap shouldEqual PendingJobMap(existingMapping)
    }

    "remove a single value" in {
      val newMap = pendingJobMap - (existingKey -> 9)
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> Seq.empty)
    }

    "add a new value to existing key (of other value type)" in {
      val newMap = pendingJobMap + (existingKey -> .1)
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> existingKeyValues.appendedAll(Seq(.1)))
    }

    "add a new key and value (of other value type)" in {
      val newMap = pendingJobMap + (newKey -> .5)
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> existingKeyValues, newKey -> Seq(.5))
    }

    "merge with other iterable" in {
      val newMap = pendingJobMap ++ Seq(newKey -> Seq(0))
      newMap shouldEqual PendingJobMap(existingMapping, existingKey -> existingKeyValues, newKey -> Seq(0))
    }

    "merge with other pending job map (of other value type)" in {
      val otherMappings: Seq[(CandidateSet, Seq[Double])] = Seq(
        existingKey -> values,
        CandidateSet.from(0, 1) -> Seq(12.34, 56.78)
      )
      val otherMap: PendingJobMap[CandidateSet, Double] = PendingJobMap.from(otherMappings)
      val newMap = pendingJobMap ++ otherMap
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
