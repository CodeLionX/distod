package com.github.codelionx.distod.types

import akka.actor.typed.ActorRef
import org.scalatest.{Matchers, WordSpec}


class PendingJobMapSpec extends WordSpec with Matchers {

  type SpecificPendingJobMap = PendingJobMap[CandidateSet, ActorRef[Any]]

  "An empty pending job map" should {
    val empty: SpecificPendingJobMap = PendingJobMap.empty
    val key: CandidateSet = CandidateSet(0)

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
  }
}
