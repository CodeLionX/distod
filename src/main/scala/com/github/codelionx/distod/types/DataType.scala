package com.github.codelionx.distod.types

import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
import java.time.{LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}

import scala.util.{Failure, Success, Try}


object DataType {

  private def orderMapping(in: DataType[_]): Int = in match {
    case null | NullType => 0
    case StringType => 1
    case DoubleType => 2
    case LocalDateType(_) => 3
    case LocalDateTimeType(_) => 4
    case ZonedDateTimeType(_) => 5
  }
}


/**
 * Represents a data type for which we define an ordering.
 *
 * @tparam T underlying (primitive) type
 */
sealed trait DataType[T <: Any] extends Ordered[DataType[_]] {

  /**
   * Parses the `value` to the underlying (primitive) type. It returns `null` for `null` values or parsing errors.
   */
  def parse(value: String): T

  /**
   * Ordering for the values of this datatype.
   */
  def valueOrdering: Ordering[T]

  /**
   * Compares two values by parsing them first to the data type. This uses the `valueOrdering` defined for the type.
   * It can be used in the `col.sortWith()` call.
   *
   * @return `true` if `v1` is less then `v2`
   */
  def valueLt(v1: String, v2: String): Boolean

  /**
   * Broaden type of this datatype.
   */
  def broaden: DataType[Any] = this.asInstanceOf[DataType[Any]]

  override def compare(that: DataType[_]): Int = DataType.orderMapping(this).compare(DataType.orderMapping(that))
}

object DateTimeType {

  /**
   * If the value is a valid date, returns either a [[com.github.codelionx.distod.types.ZonedDateTimeType]],
   * a [[com.github.codelionx.distod.types.LocalDateTimeType]], or a
   * [[com.github.codelionx.distod.types.LocalDateType]]. If the value is not valid date, `None` is returned.
   */
  def getDateTimeType(value: String): Option[DataType[_]] = {
    val checker = DateTimeChecker(value)

    if (!checker.isDate)
      None
    else if (checker.isZoned)
      Some(ZonedDateTimeType(checker.formatter))
    else if (checker.withTime)
      Some(LocalDateTimeType(checker.formatter))
    else
      Some(LocalDateType(checker.formatter))
  }
}

/**
 * Represents a [[java.time.ZonedDateTime]].
 */
final case class ZonedDateTimeType private[types](formatter: DateTimeFormatter) extends DataType[ZonedDateTime] {

  override val valueOrdering: Ordering[ZonedDateTime] = Ordering.by(_.toEpochSecond)

  override def parse(value: String): ZonedDateTime = Try {
    formatter.parse[ZonedDateTime](value, (temp: TemporalAccessor) => ZonedDateTime.from(temp))
  }.getOrElse(null)

  override def valueLt(v1: String, v2: String): Boolean = valueOrdering.lt(parse(v1), parse(v2))

}

/**
 * Represents a [[java.time.LocalDateTime]].
 */
final case class LocalDateTimeType private[types](formatter: DateTimeFormatter) extends DataType[LocalDateTime] {

  override val valueOrdering: Ordering[LocalDateTime] = Ordering.by(_.toEpochSecond(ZoneOffset.UTC))

  override def parse(value: String): LocalDateTime = Try {
    formatter.parse[LocalDateTime](value, (temp: TemporalAccessor) => LocalDateTime.from(temp))
  }.getOrElse(null)

  override def valueLt(v1: String, v2: String): Boolean = valueOrdering.lt(parse(v1), parse(v2))
}

/**
 * Represents a [[java.time.LocalDate]].
 */
final case class LocalDateType private[types](formatter: DateTimeFormatter) extends DataType[LocalDate] {

  override val valueOrdering: Ordering[LocalDate] = Ordering.by(
    _.atStartOfDay.toEpochSecond(ZoneOffset.UTC)
  )

  override def parse(value: String): LocalDate = Try {
    formatter.parse[LocalDate](value, (temp: TemporalAccessor) => LocalDate.from(temp))
  }.getOrElse(null)

  override def valueLt(v1: String, v2: String): Boolean = valueOrdering.lt(parse(v1), parse(v2))
}

/**
 * Represents a primitive [[scala.Double]], includes all other numbers (Float, Long, Int).
 */
case object DoubleType extends DataType[Double] {

  override val valueOrdering: Ordering[Double] = Ordering[Double]

  /**
   * Checks if the value is a [[scala.Double]] value.
   */
  def isDouble(value: String): Boolean = Try {
    value.toDouble
  } match {
    case Success(_) => true
    case Failure(_) => false
  }

  override def parse(value: String): Double = Try {
    value.toDouble
  }.getOrElse(.0)

  override def valueLt(v1: String, v2: String): Boolean = valueOrdering.lt(parse(v1), parse(v2))
}

/**
 * Represents a [[String]].
 *
 * Mimics a NULLS FIRST ordering by parsing `null` as `""` (empty string).
 *
 * @see [[com.github.codelionx.distod.types.NullType]]
 */
trait StringType extends DataType[String] {

  override val valueOrdering: Ordering[String] = Ordering[String]

  override def parse(value: String): String =
    if (NullType.isNull(value)) NullType.parse(value)
    else value

  override def valueLt(v1: String, v2: String): Boolean = valueOrdering.lt(parse(v1), parse(v2))
}

case object StringType extends StringType

/**
 * Represents a [[Null]] as the empty string `""`. This is a special type of the
 * [[com.github.codelionx.distod.types.StringType]].
 */
case object NullType extends StringType {

  /**
   * Checks if the value is a `null` value.
   */
  def isNull(value: String): Boolean =
    value == null || value.isEmpty || value.equalsIgnoreCase("null") || value.equalsIgnoreCase("?")

  override def parse(value: String): String = ""
}
