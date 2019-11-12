package com.github.codelionx.distod

import org.scalatest.{Matchers, WordSpec}


class MainSpec extends WordSpec with Matchers {

  "The Main object" should {
    "run once" when {
      val _ = noException shouldBe thrownBy {
        Main.main(Array.empty)
      }
    }
  }
}
