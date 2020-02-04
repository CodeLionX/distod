package com.github.codelionx.distod.io

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class ColumnProcessorSpec extends AnyWordSpec with Matchers {

  "A ColumnProcessor" should {

    "throw an exception if results are accessed before parsing is finished" in {
      val processor = ColumnProcessor(SettingsOverwrites.defaultTestSettings)
      an[IllegalAccessException] should be thrownBy processor.headers
      an[IllegalAccessException] should be thrownBy processor.columns
    }
  }
}
