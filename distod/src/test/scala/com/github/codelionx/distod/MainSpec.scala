package com.github.codelionx.distod

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, LogCapturing}
import com.github.codelionx.distod.actors.LeaderGuardian
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpec}


class MainSpec extends WordSpec with Matchers with LogCapturing {

  "distod" should {

    "compute the correct dependencies for the test.csv dataset" in {
      val inputFilePath = TestUtil.findResource("data/test.csv")
      val resultFilePath = TestUtil.findResource("data/test-results-gold.txt")
      performSystemTestFor(inputFilePath, resultFilePath)
    }

    "compute the correct dependencies for the iris.csv dataset" in {
      val inputFilePath = TestUtil.findResource("data/iris.csv")
      val resultFilePath = TestUtil.findResource("data/iris-results-gold.txt")
      performSystemTestFor(inputFilePath, resultFilePath)
    }
  }

  private def performSystemTestFor(inputFilePath: String, resultFilePath: String): Unit = {
    val config = ConfigFactory.parseString(
      s"""
         |distod.input.path = $inputFilePath
         |""".stripMargin
    ).withFallback(ConfigFactory.load("application-test"))
    val testKit = ActorTestKit(config)

    val settings = Settings(testKit.system)
    val guardProbe = testKit.createTestProbe("guard")
    val userGuardian = testKit.spawn(LeaderGuardian(), "user")
    guardProbe.expectTerminated(userGuardian)

    val distodResults = ResultFileParsing.readAndParseDistodResults(settings.outputFilePath)
    val expectedResults = ResultFileParsing.readAndParseFastodResults(resultFilePath)

    distodResults should contain theSameElementsAs expectedResults

    testKit.shutdownTestKit()
  }
}
