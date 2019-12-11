package com.github.codelionx.distod.io

import org.scalatest.{Matchers, WordSpec}

class ColumnProcessorSpec extends WordSpec with Matchers {

  "A ColumnProcessor" should {

    "throw an exception if results are accessed before parsing is finished" in {
      val processor = ColumnProcessor(SettingsOverwrites.defaultTestSettings)
      an[IllegalAccessException] should be thrownBy processor.headers
      an[IllegalAccessException] should be thrownBy processor.columns
    }
  }
}
