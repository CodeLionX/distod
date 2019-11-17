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
      partition.equivClasses should contain theSameElementsInOrderAs Seq(
        Set(0, 2, 4, 8),
        Set(1, 5),
        Set(3, 7),
        Set(6),
        Set(9)
      )
    }

    def makePartition(classes: Set[Index]*) =
      FullPartition(
        numberClasses = classes.size,
        numberElements = classes.map(_.size).sum,
        equivClasses = classes.toIndexedSeq
      )

    "compute the product correctly" in {
      {
        val partition1 = makePartition(
          Set(0, 1, 2, 5),
          Set(3),
          Set(4)
        )
        val partition2 = makePartition(
          Set(2, 5),
          Set(0, 1),
          Set(3, 4)
        )
        val expectedResult = makePartition(
          Set(0, 1),
          Set(2, 5),
          Set(3),
          Set(4)
        )
        (partition1 * partition2).equivClasses should contain theSameElementsAs expectedResult.equivClasses
      }
      {
        val partition1 = makePartition(
          Set(2, 5),
          Set(0, 1),
          Set(3, 4)
        )
        val partition2 = makePartition(
          Set(0),
          Set(1, 2),
          Set(3),
          Set(4, 5)
        )
        val expectedResult = makePartition(
          Set(0), Set(1), Set(2), Set(3), Set(4), Set(5)
        ) // empty
        (partition1 * partition2).equivClasses should contain theSameElementsAs expectedResult.equivClasses
      }
    }

    "not produce duplicate classes while computing the product" in {
      val partition1 = makePartition(
        Set(0),
        Set(1, 2),
        Set(3),
        Set(4, 5)
      )
      val partition2 = makePartition(
        Set(0, 1),
        Set(2),
        Set(3),
        Set(4, 5)
      )
      val expectedResult = makePartition(
        Set(0), Set(1), Set(2), Set(3),
        Set(4, 5)
      )
      (partition1 * partition2).equivClasses should contain theSameElementsAs expectedResult.equivClasses
    }

    "have an identity product" in {
      val newPartition = partition * partition
      newPartition shouldEqual partition
    }
  }

  "A stripped partition" should {

    def makePartition(classes: Set[Index]*) =
      StrippedPartition(
        numberClasses = classes.size,
        numberElements = classes.map(_.size).sum,
        equivClasses = classes.toIndexedSeq
      )

    "not contain classes with only one element" in {
      val partition = Partition.strippedFrom(column)
      partition.equivClasses.filter { elems =>
        elems.size == 1
      } shouldBe empty
    }

    "compute the product correctly" in {
      {
        val partition1 = makePartition(
          Set(0, 1, 2, 5)
        )
        val partition2 = makePartition(
          Set(2, 5),
          Set(0, 1),
          Set(3, 4)
        )
        val expectedResult = makePartition(
          Set(0, 1),
          Set(2, 5)
        )
        (partition1 * partition2).equivClasses should contain theSameElementsAs expectedResult.equivClasses
      }
      {
        val partition1 = makePartition(
          Set(2, 5),
          Set(0, 1),
          Set(3, 4)
        )
        val partition2 = makePartition(
          Set(1, 2),
          Set(4, 5)
        )
        val expectedResult = makePartition() // empty
        (partition1 * partition2).equivClasses should contain theSameElementsAs expectedResult.equivClasses
      }
    }

    "have an identity product" in {
      val partition = Partition.strippedFrom(column)
      val newPartition = partition * partition
      newPartition shouldEqual partition
    }
  }
}
