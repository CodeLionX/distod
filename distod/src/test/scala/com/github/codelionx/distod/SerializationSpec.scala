package com.github.codelionx.distod

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializationExtension
import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.{Matchers, WordSpec}


class SerializationSpec extends WordSpec with Matchers {

  "The serialization system" should {
    val system = akka.actor.typed.ActorSystem(Behaviors.empty, "serialization-test-system")
    val serialization = SerializationExtension(system.toClassic)

    "serialize CandidateSets" in {
      val testSubjects = Seq(
        CandidateSet.empty,
        CandidateSet.from(0, 5, 8, 2, 4),
        CandidateSet.fromSpecific(0 until 200 by 5)
      )

      testSubjects.foreach(testSubject =>
        noException should be thrownBy {
          println(testSubject.toBitMask.map(_.toBinaryString).mkString(" "))
          // turn it into bytes
          val bytes = serialization.serialize(testSubject).get

          // turn it back into an object
          val back = serialization.deserialize(bytes, classOf[CandidateSet]).get
          back shouldEqual testSubject
        }
      )
    }
  }
}
