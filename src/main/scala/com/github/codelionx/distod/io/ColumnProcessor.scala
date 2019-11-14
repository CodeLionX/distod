package com.github.codelionx.distod.io

import com.github.codelionx.distod.Settings.InputParsingSettings
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.AbstractRowProcessor

import scala.collection.mutable


object ColumnProcessor {

  /**
   * Creates a new ColumnProcessor
   */
  def apply(settings: InputParsingSettings): ColumnProcessor = new ColumnProcessor(settings)
}


class ColumnProcessor private(settings: InputParsingSettings) extends AbstractRowProcessor {

  private var parsedLines: Long = 0
  private var numberOfColumns: Int = _
  private var parsingEnded: Boolean = false

  private var _columns: Array[mutable.Buffer[String]] = _
  private var _headers: Array[String] = _

  private def generateSyntheticColumnNames(length: Int): Array[String] = {
    // seems very fast, so no optimization necessary
    val columnIndexToName = (i: Int) => {
      var name = ""
      var number = i
      while (number > 0) {
        val remainder = number % 26
        if (remainder == 0) {
          name += "Z"
          number = (number / 26) - 1
        } else {
          name += ('A' + remainder - 1).toChar
          number /= 26
        }
      }
      name.reverse
    }
    (1 to length).map(columnIndexToName).toArray
  }

  private def lazyInit(row: Array[String], context: ParsingContext): Unit = {
    numberOfColumns = settings.maxColumns match {
      case Some(limit) => Math.min(limit, row.length)
      case None => row.length
    }
    _headers =
      if (settings.hasHeader)
        context.headers().take(numberOfColumns)
      else
        generateSyntheticColumnNames(numberOfColumns)

    val bufferSizeHint = settings.maxRows match {
      case Some(limit) => limit
      case None => mutable.ArrayBuffer.DefaultInitialSize
    }
    _columns = Array.fill(numberOfColumns) {
      new mutable.ArrayBuffer(bufferSizeHint)
    }
  }

  def columns: Array[Array[String]] =
    if (parsingEnded) _columns.map(_.toArray)
    else throw new IllegalAccessException("The parsing process has not finished yet!")

  def headers: Array[String] =
    if (parsingEnded) _headers
    else throw new IllegalAccessException("The parsing process has not finished yet!")

  // from RowProcessor
//  override def processStarted(context: ParsingContext): Unit = {}

  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    if(_headers == null || _columns == null) {
      lazyInit(row, context)
    }
    if (settings.maxRows.fold(ifEmpty = false)(parsedLines >= _)) {
      println(s"Stopping parsing early because maxRows = ${settings.maxRows} was specified. Current line: ${context.currentLine()}")
      context.stop()
    } else {
      for (j <- row.indices.take(numberOfColumns)) {
        _columns(j).append(row(j))
      }
    }
    parsedLines += 1
  }


  override def processEnded(context: ParsingContext): Unit = {
    parsingEnded = true
  }
}
