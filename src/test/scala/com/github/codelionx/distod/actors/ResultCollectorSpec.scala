package com.github.codelionx.distod.actors

import java.io.{BufferedWriter, StringWriter}

import akka.actor.testkit.typed.scaladsl.{FishingOutcomes, LogCapturing, ScalaTestWithActorTestKit}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{AckBatch, DependencyBatch, ResultCommand, ResultProxyCommand}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.{ConstantOrderDependency, EquivalencyOrderDependency}
import com.typesafe.config.ConfigFactory
import org.scalatest
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps


class ResultCollectorSpec
  extends ScalaTestWithActorTestKit(ConfigFactory.load()) with WordSpecLike with LogCapturing with Matchers {

  val writeBuffer = new StringWriter()

  val patchedResultCollectorBehavior: Behavior[ResultCommand] = Behaviors.setup[ResultCommand] { context =>
    new ResultCollector(context) {
      override protected def createWriter(append: Boolean): BufferedWriter = new BufferedWriter(writeBuffer)
    }.start()
  }

  "The result collector actor" should {
    val collector = spawn(patchedResultCollectorBehavior, ResultCollector.name)

    val deps = Seq(
      ConstantOrderDependency(CandidateSet.empty, 0),
      ConstantOrderDependency(CandidateSet.empty, 3),
      ConstantOrderDependency(CandidateSet.from(0, 2), 1),
      EquivalencyOrderDependency(CandidateSet.from(0, 2), 3, 4, reverse = true)
    )
    val expectedFileContent = deps.map(_.toString).mkString("", "\n", "\n")

    def testFileContents(expected: String = expectedFileContent): scalatest.Assertion = {
      val fileContent = writeBuffer.toString
      fileContent shouldEqual expected
    }

    "register at the receptionist" in {
      val probe = createTestProbe[Receptionist.Listing]()
      system.receptionist ! Receptionist.Subscribe(ResultCollector.CollectorServiceKey, probe.ref)

      val messages = probe.fishForMessage(2 seconds) {
        case ResultCollector.CollectorServiceKey.Listing(listing) if listing.isEmpty =>
          FishingOutcomes.continueAndIgnore
        case ResultCollector.CollectorServiceKey.Listing(listing) if listing.nonEmpty =>
          FishingOutcomes.complete
      }
      messages.head.serviceInstances(ResultCollector.CollectorServiceKey) should contain(collector)
    }

    "acknowledge batches and write them immediately" in {
      val probe = createTestProbe[ResultProxyCommand]()
      collector ! DependencyBatch(0, deps, probe.ref)
      probe.expectMessageType[AckBatch]

      testFileContents()
    }

    "ignore duplicate batches from the same sender, but send an acknowledgement" in {
      val batch = Seq(ConstantOrderDependency(CandidateSet.from(6), 1))
      val probe = createTestProbe[ResultProxyCommand]()
      collector ! DependencyBatch(0, batch, probe.ref)
      probe.expectMessageType[AckBatch]
      collector ! DependencyBatch(0, batch, probe.ref)
      probe.expectMessageType[AckBatch]

      testFileContents(
        expectedFileContent + batch.map(_.toString).mkString("", "\n", "\n")
      )
    }
  }
}
