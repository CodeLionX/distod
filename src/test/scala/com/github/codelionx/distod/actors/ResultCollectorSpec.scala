package com.github.codelionx.distod.actors

import java.io.{BufferedWriter, StringWriter}

import akka.actor.testkit.typed.scaladsl.{FishingOutcomes, LogCapturing, ScalaTestWithActorTestKit}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import com.github.codelionx.distod.io.ColumnProcessor
import com.github.codelionx.distod.protocols.ResultCollectionProtocol._
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.{ConstantOrderDependency, EquivalencyOrderDependency}
import org.scalatest
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps


class ResultCollectorSpec
  extends ScalaTestWithActorTestKit() with WordSpecLike with Matchers with LogCapturing {

  val writeBuffer = new StringWriter()

  val patchedResultCollectorBehavior: Behavior[ResultCommand] = Behaviors.setup[ResultCommand] { context =>
    new ResultCollector(context) {
      override protected def createWriter(append: Boolean): BufferedWriter = new BufferedWriter(writeBuffer)
    }.start()
  }

  "The result collector actor" should {
    val collector = spawn(patchedResultCollectorBehavior, ResultCollector.name)

    val deps1 = Seq(
      ConstantOrderDependency(CandidateSet.empty, 0),
      ConstantOrderDependency(CandidateSet.empty, 3),
    )
    val deps2 = Seq(
      ConstantOrderDependency(CandidateSet.from(0, 2), 1),
      EquivalencyOrderDependency(CandidateSet.from(0, 2), 3, 4, reverse = true),
    )
    val deps3 = Seq(
      ConstantOrderDependency(CandidateSet.from(6), 1)
    )
    val columnNames = ColumnProcessor.generateSyntheticColumnNames(10)
    val expectedFileContent = (deps1 ++ deps2).map(_.withAttributeNames(columnNames)).mkString("", "\n", "\n")

    def testFileContents(expected: String = expectedFileContent): scalatest.Assertion = {
      val fileContent = writeBuffer.toString

      println(fileContent)
      println(expected)
      fileContent shouldEqual expected
    }

    "register at the receptionist" in {
      val probe = createTestProbe[Receptionist.Listing]()
      system.receptionist ! Receptionist.Subscribe(ResultCollector.CollectorServiceKey, probe.ref)

      val messages = probe.fishForMessage(1 second) {
        case ResultCollector.CollectorServiceKey.Listing(listing) if listing.isEmpty =>
          FishingOutcomes.continueAndIgnore
        case ResultCollector.CollectorServiceKey.Listing(listing) if listing.nonEmpty =>
          FishingOutcomes.complete
      }
      messages.head.serviceInstances(ResultCollector.CollectorServiceKey) should contain(collector)
    }

    "acknowledge batches and buffer them if no attributes have been set" in {
      val probe = createTestProbe[ResultProxyCommand]()
      collector ! DependencyBatch(0, deps1, probe.ref)
      probe.expectMessageType[AckBatch]
    }

    "write all buffered results if attributes are received" in {
      collector ! SetAttributeNames(columnNames)

      val probe = createTestProbe[ResultProxyCommand]()
      collector ! DependencyBatch(1, deps2, probe.ref)
      probe.expectMessageType[AckBatch]

      testFileContents()
    }

    "ignore duplicate batches from the same sender, but send an acknowledgement" in {
      val probe = createTestProbe[ResultProxyCommand]()
      collector ! DependencyBatch(0, deps3, probe.ref)
      probe.expectMessageType[AckBatch]
      collector ! DependencyBatch(0, deps3, probe.ref)
      probe.expectMessageType[AckBatch]

      testFileContents(
        expectedFileContent + deps3.map(_.withAttributeNames(columnNames)).mkString("", "\n", "\n")
      )
    }
  }
}
