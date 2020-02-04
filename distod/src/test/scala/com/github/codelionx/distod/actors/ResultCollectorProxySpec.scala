package com.github.codelionx.distod.actors

import akka.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import akka.actor.typed.receptionist.Receptionist
import com.github.codelionx.distod.protocols.ResultCollectionProtocol._
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.{ConstantOrderDependency, EquivalencyOrderDependency}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike


object ResultCollectorProxySpec {

  def config: Config = ConfigFactory.parseString(
    "distod.result-batch-size = 5"
  ).withFallback(ConfigFactory.load("application-test.conf"))
}


class ResultCollectorProxySpec
  extends ScalaTestWithActorTestKit(ResultCollectorProxySpec.config)
    with AnyWordSpecLike
    with LogCapturing
    with Matchers {

  private val deps = Seq(
    ConstantOrderDependency(CandidateSet.empty, 0),
    ConstantOrderDependency(CandidateSet.empty, 3),
    ConstantOrderDependency(CandidateSet.from(0, 2), 1),
    EquivalencyOrderDependency(CandidateSet.from(0, 2), 3, 4, reverse = true)
  )
  private val additionalDeps = Seq(ConstantOrderDependency(CandidateSet.from(1, 2), 15))

  "A result collector proxy" should {
    val proxy = spawn(ResultCollectorProxy())
    val targetProbe = createTestProbe[ResultCommand](ResultCollector.name)

    "stash all messages until result collector gets available" in {
      // send first batches
      proxy ! FoundDependencies(deps)
      proxy ! FoundDependencies(additionalDeps)
      Thread.sleep(500)

      // now register target
      system.receptionist ! Receptionist.Register(ResultCollector.CollectorServiceKey, targetProbe.ref)
      targetProbe.expectMessage(DependencyBatch(0, deps ++ additionalDeps, proxy))
      proxy ! AckBatch(0)
    }

    "buffer messages until batch size is reached" in {
      // send first deps
      proxy ! FoundDependencies(deps)
      targetProbe.expectNoMessage()

      // send second batch (sum >= 5)
      proxy ! FoundDependencies(additionalDeps)
      targetProbe.expectMessage(DependencyBatch(1, deps ++ additionalDeps, proxy))
    }

    "resend batches that were not acked" in {
      targetProbe.expectMessage(DependencyBatch(1, deps ++ additionalDeps, proxy))
      proxy ! AckBatch(1)
    }

    "flush buffered dependencies before shutting down" in {
      val stopperProbe = createTestProbe[FlushFinished.type]("stopper")
      // send some deps
      proxy ! FoundDependencies(deps)
      targetProbe.expectNoMessage()

      // send flush and stop command
      proxy ! FlushAndStop(stopperProbe.ref)
      targetProbe.expectMessage(DependencyBatch(2, deps, proxy))

      // flush should only be finished if all sent batches are acked
      Thread.sleep(500)
      stopperProbe.expectNoMessage()

      proxy ! AckBatch(2)
      stopperProbe.expectMessage(FlushFinished)
      stopperProbe.expectTerminated(proxy)
    }

    "immediately stop if buffer is empty" in {
      val proxy = spawn(ResultCollectorProxy())
      val stopperProbe = createTestProbe[FlushFinished.type]("stopper")

      proxy ! FlushAndStop(stopperProbe.ref)
      stopperProbe.expectMessage(FlushFinished)
      stopperProbe.expectTerminated(proxy)
    }
  }
}
