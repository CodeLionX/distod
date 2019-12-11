package com.github.codelionx.distod.io

import com.github.codelionx.distod.Settings.InputParsingSettings

object SettingsOverwrites {


  case class InputSettingsOverwrite(
                                     filePath: String,
                                     hasHeader: Boolean,
                                     maxColumns: Option[Int],
                                     maxRows: Option[Int]
                                   ) extends InputParsingSettings


  val defaultTestSettings: InputSettingsOverwrite = InputSettingsOverwrite(
    filePath = this.getClass.getClassLoader.getResource("data/test.csv").getPath,
    hasHeader = false,
    maxColumns = None,
    maxRows = None
  )
}
