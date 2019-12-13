package com.github.codelionx.distod.io

import org.scalatest.{Matchers, WordSpec}


class CSVParserSpec extends WordSpec with Matchers {

  "The CSVParser" should {

    "parse a CSV file without headers by substituting them" in {
      val table = CSVParser(SettingsOverwrites.defaultTestSettings).parse()

      table.name shouldEqual "test"
      table.headers shouldEqual Array("A", "B", "C", "D")
      table.columns shouldEqual Array(
        Array("a", "b", "a", "b"),
        Array("a", "b", "c", "c"),
        Array("a", "a", "a", "d"),
        Array("0", "0", "2", "4")
      )
    }

    "parse a CSV file with headers" in {
      val settings = SettingsOverwrites.defaultTestSettings.copy(
        hasHeader = true
      )
      val table = CSVParser(settings).parse()

      table.name shouldEqual "test"
      table.headers shouldEqual Array("a", "a", "a", "0")
      table.columns shouldEqual Array(
        Array("b", "a", "b"),
        Array("b", "c", "c"),
        Array("a", "a", "d"),
        Array("0", "2", "4")
      )
    }

    "stop parsing early in the case of limited rows" in {
      val settings = SettingsOverwrites.defaultTestSettings.copy(
        maxRows = Some(2)
      )
      val table = CSVParser(settings).parse()

      table.columns.map(c =>
        c.length should be <= 2
      )
    }

    "limit the number of parsed columns if specified" in {
      val settings = SettingsOverwrites.defaultTestSettings.copy(
        maxColumns = Some(2)
      )
      val table = CSVParser(settings).parse()

      table.headers.length should be <= 2
      table.columns.length should be <= 2
    }
  }
}
