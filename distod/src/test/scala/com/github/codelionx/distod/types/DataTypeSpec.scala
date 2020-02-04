package com.github.codelionx.distod.types

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class DataTypeSpec extends AnyWordSpec with Matchers {

  "The StringType" should {
    val dt = StringType

    "parse nulls correctly as empty string" when {

      def shouldParseToNull(value: String): Unit = {
        s"the value is `${if (value == null) "literal<null>" else value}`" in {
          val nullValue = dt.parse(value)
          nullValue shouldBe ""
        }
      }

      Seq("null", "?", "", null).foreach(value =>
        shouldParseToNull(value)
      )
    }

    "parse correct strings as identity" in {
      val value = "`!value1<$2²#+."
      val parsed = dt.parse(value)

      parsed shouldBe value
      parsed shouldEqual value
    }
  }
}
