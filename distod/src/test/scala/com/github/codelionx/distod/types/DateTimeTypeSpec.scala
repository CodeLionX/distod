package com.github.codelionx.distod.types

import java.time.format.DateTimeFormatter._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class DateTimeTypeSpec extends AnyWordSpec with Matchers {

  import DateTimeType._


  "A DateTimeType" should {

    "identify datetimes with time zones correctly" in {
      getDateTimeType("2019-12-27T22:15:30+02:00") shouldEqual Some(ZonedDateTimeType(ISO_DATE_TIME))
      getDateTimeType("Tue, 3 Jun 2008 11:05:30 GMT") shouldEqual Some(ZonedDateTimeType(RFC_1123_DATE_TIME))
      getDateTimeType("10 Jun 2019 08:05:30 GMT") shouldEqual Some(ZonedDateTimeType(RFC_1123_DATE_TIME))
    }

    "identify datetimes without time zone" in {
      getDateTimeType("2011-12-03T10:15:30") shouldEqual Some(LocalDateTimeType(ISO_DATE_TIME))
    }

    "identify dates" in {
      // dates are always local (without zone)
      getDateTimeType("2011-12-03") shouldEqual Some(LocalDateType(ISO_DATE))
      getDateTimeType("2011-12-03+01:00") shouldEqual Some(LocalDateType(ISO_DATE))
      getDateTimeType("01/28/1997") shouldEqual Some(LocalDateType(DateTimeChecker.customUSDateFormatter))
      getDateTimeType("18.11.1994") shouldEqual Some(LocalDateType(DateTimeChecker.customGermanDateFormatter))
    }

    "not infer 0-padded number with 6 digits as date" in {
      getDateTimeType("000023") shouldEqual None
    }

    "not infer 0-padded number with 8 digits as date" in {
      getDateTimeType("00000023") shouldEqual None
    }
  }
}
