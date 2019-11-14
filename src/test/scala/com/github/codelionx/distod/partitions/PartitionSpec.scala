package com.github.codelionx.distod.partitions

import org.scalatest.{Matchers, WordSpec}


class PartitionSpec extends WordSpec with Matchers {

  val column: Array[String] = Array("a", "b", "a", "c", "a", "b", "d", "c", "a", "e")

  "A full partition" should {
    val partition = Partition.fullFrom(column)

    "contain all values of the column" in {
      partition.numberElements shouldBe column.length
    }

    "have the right number of classes" in {
      partition.numberClasses shouldBe 5
    }

    "sort the classes" in {
      partition.equivClasses.keys.toSeq should contain theSameElementsInOrderAs (0 until 5)
    }
  }

  "A stripped partition" should {
    "not contain classes with only one element" in {
      val partition = Partition.strippedFrom(column)
      partition.equivClasses.filter{ case (_, elems) =>
        elems.size == 1
      } shouldBe empty
    }
  }
}
