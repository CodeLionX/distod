package com.github.codelionx.distod.io

import com.github.codelionx.distod.Settings.InputParsingSettings
import com.github.codelionx.distod.TestUtil


object SettingsOverwrites {


  case class InputSettingsOverwrite(
      filePath: String,
      hasHeader: Boolean,
      maxColumns: Option[Int],
      maxRows: Option[Int]
  ) extends InputParsingSettings


  val defaultTestSettings: InputSettingsOverwrite = InputSettingsOverwrite(
    filePath = TestUtil.findResource("data/test.csv"),
    hasHeader = false,
    maxColumns = None,
    maxRows = None
  )
}
