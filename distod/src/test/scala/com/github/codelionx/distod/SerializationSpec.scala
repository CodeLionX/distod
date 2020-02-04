package com.github.codelionx.distod

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import akka.serialization.SerializationExtension
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.partitionMgmt.channel.{AttributeData, DataMessage, EmptyPartitionData, FullPartitionData}
import com.github.codelionx.distod.partitions.Partition
import com.github.codelionx.distod.types.CandidateSet
import com.typesafe.config.ConfigFactory
import org.scalatest.TryValues._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

import scala.language.postfixOps
import scala.util.Success


object SerializationSpec {

  final case class Message(id: CandidateSet) extends CborSerializable

}

class SerializationSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  import SerializationSpec._


  private val testkit = ActorTestKit(
    ConfigFactory.load("application-test")
      .withFallback(ConfigFactory.defaultApplication())
      .resolve()
  )

  override protected def afterAll(): Unit =
    testkit.shutdownTestKit()

  "The serialization system" should {

    "serialize messages with CandidateSets" in {
      val serialization = SerializationExtension(testkit.system)
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

  "Data messages" when {
    implicit val system: ActorSystem[_] = testkit.system

    val attributes = 0 until 10
    val candidate: CandidateSet = CandidateSet.from(0)
    val partition = Partition.fullFrom(Array(
      "0", "1", "0", "1", "1", "2", "0", "3", "2", "0", "4", "1", "0", "5", "8"
    ))
    val attributeData = AttributeData(attributes)
    val fullData = FullPartitionData(candidate, partition)
    val emptyData = EmptyPartitionData(partition.stripped)

    "being an AttributeData message" should {
      "be serializable" in {
        val bytes = DataMessage.serialize(attributeData)
        val obj = DataMessage.deserialize(bytes)
        obj shouldEqual attributeData
      }
    }

    "being a FullPartitionData message" should {
      "be serializable" in {
        val bytes = DataMessage.serialize(fullData)
        val obj = DataMessage.deserialize(bytes)
        obj shouldEqual fullData
      }
    }

    "being an EmptyPartitionData message" should {
      "be serializable" in {
        val bytes = DataMessage.serialize(emptyData)
        val obj = DataMessage.deserialize(bytes)
        obj shouldEqual emptyData
      }
    }
  }
}
