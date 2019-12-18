package com.github.codelionx.distod

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializationExtension
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.types.CandidateSet
import com.typesafe.config.ConfigFactory
import org.scalatest.TryValues._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.language.postfixOps
import scala.util.Success

object SerializationSpec {

  final case class Message(id: CandidateSet) extends CborSerializable

}

class SerializationSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  import SerializationSpec._

  val testkit = ActorTestKit(
    ConfigFactory.load("application-test")
      .withFallback(ConfigFactory.defaultApplication())
      .resolve()
  )

  override protected def afterAll(): Unit =
    testkit.shutdownTestKit()

  "The serialization system" should {

    "serialize messages with CandidateSets" in {
      val serialization = SerializationExtension(testkit.system.toClassic)
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
