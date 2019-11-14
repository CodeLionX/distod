package com.github.codelionx.distod.types

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.{LocalDate, LocalDateTime, ZonedDateTime}
import java.util.Locale

import scala.util.{Failure, Success, Try}


object DateTimeChecker {

  // custom formatters
  val customUSDateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendPattern("MM/dd/yyyy")
    .toFormatter(Locale.US)
  val customGermanDateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendPattern("dd.MM.yyyy")
    .toFormatter(Locale.GERMAN)

  /**
   * Supported datetime formats.
   */
  val datetimeFormats: Seq[DateTimeFormatter] = Seq(
    DateTimeFormatter.ISO_DATE_TIME,
    DateTimeFormatter.RFC_1123_DATE_TIME
  )

  /**
   * Supported date formats (without time info).
   */
  val dateFormats: Seq[DateTimeFormatter] = Seq(
    customGermanDateFormatter,
    customUSDateFormatter,
    DateTimeFormatter.ISO_DATE,
    //    BasicFormat,
    DateTimeFormatter.ISO_LOCAL_DATE
  )

  /**
   * Checks the `value` to be of a known date or time format. See
   * [[com.github.codelionx.distod.types.DateTimeChecker#dateFormats]] and
   * [[com.github.codelionx.distod.types.DateTimeChecker#datetimeFormats]] for supported formats.
   */
  private[types] def apply(value: String): DateTimeChecker = new DateTimeChecker(value)
}


private[types] class DateTimeChecker {

  import DateTimeChecker._


  /**
   * If the `value` is a valid date
   */
  var isDate: Boolean = false
  /**
   * Only use if `isValid` is `true`!
   *
   * If the date format contains Zone information.
   */
  var isZoned: Boolean = false
  /**
   * Only use if `isValid` is `true`!
   *
   * If the date format contains time information.
   */
  var withTime: Boolean = false
  /**
   * Only use if `isValid` is `true`!
   *
   * The formatter that can produce and parse this date.
   */
  var formatter: DateTimeFormatter = _

  private[DateTimeChecker] def this(value: String) = {
    this()
    checkForDateTime(value)
    if (!isDate)
      checkForDate(value)
  }

  private def checkForDateTime(value: String): Unit = {
    for (format <- datetimeFormats) {
      Try {
        ZonedDateTime.parse(value, format)
        this.formatter = format
        this.isZoned = true
      } recoverWith {
        case _: Throwable =>
          Try {
            LocalDateTime.parse(value, format)
            this.formatter = format
            this.isZoned = false
          }
      } match {
        case Success(_) =>
          this.isDate = true
          this.withTime = true
          return
        case Failure(_) =>
      }
    }
  }

  private def checkForDate(value: String): Unit = {
    for (format <- dateFormats) {
      Try {
        LocalDate.parse(value, format)
        this.formatter = format
        this.isZoned = false
      } match {
        case Success(_) =>
          this.isDate = true
          return
        case Failure(_) =>
      }
    }
  }
}
