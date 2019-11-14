package com.github.codelionx.distod.io

import java.io.File

import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.Settings.InputParsingSettings
import com.github.codelionx.distod.types.Table
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

import scala.io.Codec


/**
 * CSV Parser for reading input files and parsing them into a table representation.
 */
object CSVParser {

  def apply(settings: Settings): CSVParser = new CSVParser(settings.inputParsingSettings)

  def apply(settings: InputParsingSettings): CSVParser = new CSVParser(settings)

}

class CSVParser private(settings: InputParsingSettings) {

  private implicit val fileCodec: Codec = Codec.UTF8

  private val parserSettings = {
    val s = new CsvParserSettings
    s.detectFormatAutomatically()
    s.setHeaderExtractionEnabled(settings.hasHeader)
    s
  }

  /**
   * Reads the CSV file specified in the settings supplied to the constructor.
   *
   * @return
   */
  def parse(): Table = parse(new File(settings.filePath))

  /**
   * Reads a CSV file and parses it.
   *
   * @param file file name, can contain relative or absolute paths, see [[java.io.File]] for more infos
   * @return
   */
  def parse(file: String): Table = parse(new File(file))

  /**
   * Reads a CSV file and parses it.
   *
   * @param file [[java.io.File]] pointing to the dataset
   * @return
   */
  def parse(file: File): Table = {
    val p = ColumnProcessor(settings)
    val s = parserSettings.clone()
    s.setProcessor(p)
    val parser = new CsvParser(s)

    // parse and return result
    parser.parse(file)
    Table(
      name = settings.filePath.substring(0, settings.filePath.lastIndexOf(".")),
      headers = p.headers,
      columns = p.columns
    )
  }
}
