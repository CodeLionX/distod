package com.github.codelionx.distod.io

import com.github.codelionx.distod.types._
import java.time.format.DateTimeFormatter._

import com.github.codelionx.distod.partitions.TypeInferrer
import org.scalatest.{Matchers, WordSpec}


class TypeInferrerSpec extends WordSpec with Matchers {

  "A TypeInferrer" should {
    "infer doubles" in {
      TypeInferrer.inferType("1.2") shouldEqual DoubleType
      TypeInferrer.inferType("123.") shouldEqual DoubleType
      TypeInferrer.inferType(".123") shouldEqual DoubleType
    }

    "infer longs as doubles" in {
      TypeInferrer.inferType(Long.MaxValue.toString) shouldEqual DoubleType
    }

    "infer ints as doubles" in {
      TypeInferrer.inferType("123876") shouldEqual DoubleType
      TypeInferrer.inferType(Int.MaxValue.toString) shouldEqual(DoubleType)
    }

    "infer datetimes with time zone" in {
      TypeInferrer.inferType("2019-12-27T22:15:30+02:00") shouldEqual ZonedDateTimeType(ISO_DATE_TIME)
      TypeInferrer.inferType("Tue, 3 Jun 2008 11:05:30 GMT") shouldEqual ZonedDateTimeType(RFC_1123_DATE_TIME)
      TypeInferrer.inferType("10 Jun 2019 08:05:30 GMT") shouldEqual ZonedDateTimeType(RFC_1123_DATE_TIME)
    }

    "infer datetimes without time zone" in {
      TypeInferrer.inferType("2011-12-03T10:15:30") shouldEqual LocalDateTimeType(ISO_DATE_TIME)
    }

    "infer dates" in {
      // dates are always local (without zone)
      TypeInferrer.inferType("2011-12-03") shouldEqual LocalDateType(ISO_DATE)
      TypeInferrer.inferType("2011-12-03+01:00") shouldEqual LocalDateType(ISO_DATE)
      TypeInferrer.inferType("01/28/1997") shouldEqual LocalDateType(DateTimeChecker.customUSDateFormatter)
      TypeInferrer.inferType("18.11.1994") shouldEqual LocalDateType(DateTimeChecker.customGermanDateFormatter)
    }

    "infer strings" in {
      TypeInferrer.inferType("some string") shouldEqual StringType
      TypeInferrer.inferType("Monday, 2011/03/12") shouldEqual StringType
    }

    "infer NULLS as strings" in {
      TypeInferrer.inferType(null) shouldEqual StringType
      TypeInferrer.inferType("") shouldEqual StringType
      TypeInferrer.inferType("null") shouldEqual StringType
      TypeInferrer.inferType("?") shouldEqual StringType
    }

    "infer doubles in a column" in {
      val column = Array("1.2", "", "1.3", null, null, "0.1243", "24", "5645456", "14214.1")

      TypeInferrer.inferTypeForColumn(column) shouldEqual DoubleType
    }

    "infer dates in a column" in {
      val column = Array("2011-12-06+01:00", "2011-12-03+00:00", "2011-12-07+01:00", "2012-01-03+06:00", "2011-11-28+03:00")

      TypeInferrer.inferTypeForColumn(column) shouldEqual LocalDateType(ISO_DATE)
    }

    // bugfix guides
    "not infer 0-padded number with 6 digits as date" in {
      TypeInferrer.inferType("000023") shouldEqual DoubleType
    }

    "not infer 0-padded number with 8 digits as date" in {
      TypeInferrer.inferType("00000023") shouldEqual DoubleType
    }

  }
}