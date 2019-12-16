package com.github.codelionx.distod

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializationExtension
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.Serialization.CborSerializable
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.TryValues._

import scala.util.Success


object SerializationSpec {
  final case class Message(id: CandidateSet) extends CborSerializable
}

class SerializationSpec extends WordSpec with Matchers {

  import SerializationSpec._


  "The serialization system" should {
    val system = akka.actor.typed.ActorSystem(Behaviors.empty, "serialization-test-system")
    val serialization = SerializationExtension(system.toClassic)

    "serialize messages with CandidateSets" in {
      val testSubjects = Seq(
        CandidateSet.empty,
        CandidateSet.from(0, 5, 8, 2, 4),
        CandidateSet.fromSpecific(0 until 200 by 5)
      ).map(Message)

      testSubjects.foreach(testSubject =>
        noException should be thrownBy {
          // turn it into bytes
          val bytes = serialization.serialize(testSubject).success.value

          // turn it back into an object
          val back = serialization.deserialize(bytes, classOf[Message])
          back shouldEqual Success(testSubject)
        }
      )
    }
  }
}
