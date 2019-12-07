package com.github.codelionx.distod

import akka.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import com.github.codelionx.distod.actors.LeaderGuardian
import org.scalatest.{Matchers, WordSpecLike}


class MainSpec extends ScalaTestWithActorTestKit() with WordSpecLike with Matchers with LogCapturing {

  "distod" should {

    "compute the correct dependencies" in {
      val resultFilePath = "src/test/resources/data/test-results-gold.txt"
      val settings = Settings(system)
      val guardProbe = createTestProbe("guard")
      val userGuardian = spawn(LeaderGuardian(), "user")
      guardProbe.expectTerminated(userGuardian)

      val distodResults = ResultFileParsing.readAndParseDistodResults(settings.outputFilePath)
      val expectedResults = ResultFileParsing.readAndParseFastodResults(resultFilePath)

      distodResults should contain theSameElementsAs expectedResults
    }

  }
}
