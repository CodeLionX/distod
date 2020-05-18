package com.github.codelionx.distod.partitions

import com.github.codelionx.distod.types.TupleValueMap
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class PartitionSpec extends AnyWordSpec with Matchers {

  val column: Array[String] = Array("a", "b", "a", "c", "a", "b", "d", "c", "a", "e")

  "A full partition" should {
    val partition = Partition.fullFrom(column)

    "contain all values of the column that were not stripped" in {
      partition.numberElements shouldBe 8
    }

    "have the right number of classes" in {
      partition.numberClasses shouldBe 3
    }

    "sort the classes" in {
      partition.equivClasses should contain theSameElementsInOrderAs Seq(
        Array(0, 2, 4, 8),
        Array(1, 5),
        Array(3, 7),
      )
    }

    def makePartition(sets: Set[Int]*) = {
      val classes = sets.map(set => Array.from(set)).toArray
      FullPartition(
        nTuples = classes.map(_.length).iterator.sum,
        tupleValueMap = Partition.convertToTupleValueMap(classes)
      )
    }

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

    def makePartition(sets: Set[Index]*) = {
      val classes = sets.map(set => Array.from(set)).toArray
      StrippedPartition(
        nTuples = 6,
        numberClasses = classes.length,
        numberElements = classes.map(_.length).sum,
        equivClasses = classes
      )
    }

    "not contain classes with only one element" in {
      val partition = Partition.strippedFrom(column)
      partition.equivClasses.filter { elems =>
        elems.length == 1
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

  "The partition product" should {
    "have the same result than FASTOD" in {
      val expectedClasses = IndexedSeq(
        Set(2, 29), Set(11, 24), Set(12, 45), Set(9, 34, 37), Set(7, 26), Set(40, 43),
        Set(0, 17), Set(19, 44, 46), Set(5, 16), Set(20, 31), Set(80, 81), Set(66, 88),
        Set(55, 99), Set(67, 82, 101, 142), Set(61, 149), Set(62, 119), Set(71, 73),
        Set(91, 127), Set(56, 100), Set(72, 146), Set(51, 115), Set(128, 132), Set(104, 116, 147),
        Set(65, 86, 140), Set(77, 145), Set(124, 144), Set(52, 139, 141)
      ).map(
        s => Array.from(s)
      )

      val part0 = Partition.fullFrom(Array(
        "5.1", "4.9", "4.7", "4.6", "5.0", "5.4", "4.6", "5.0", "4.4", "4.9", "5.4", "4.8",
        "4.8", "4.3", "5.8", "5.7", "5.4", "5.1", "5.7", "5.1", "5.4", "5.1", "4.6", "5.1",
        "4.8", "5.0", "5.0", "5.2", "5.2", "4.7", "4.8", "5.4", "5.2", "5.5", "4.9", "5.0",
        "5.5", "4.9", "4.4", "5.1", "5.0", "4.5", "4.4", "5.0", "5.1", "4.8", "5.1", "4.6",
        "5.3", "5.0", "7.0", "6.4", "6.9", "5.5", "6.5", "5.7", "6.3", "4.9", "6.6", "5.2",
        "5.0", "5.9", "6.0", "6.1", "5.6", "6.7", "5.6", "5.8", "6.2", "5.6", "5.9", "6.1",
        "6.3", "6.1", "6.4", "6.6", "6.8", "6.7", "6.0", "5.7", "5.5", "5.5", "5.8", "6.0",
        "5.4", "6.0", "6.7", "6.3", "5.6", "5.5", "5.5", "6.1", "5.8", "5.0", "5.6", "5.7",
        "5.7", "6.2", "5.1", "5.7", "6.3", "5.8", "7.1", "6.3", "6.5", "7.6", "4.9", "7.3",
        "6.7", "7.2", "6.5", "6.4", "6.8", "5.7", "5.8", "6.4", "6.5", "7.7", "7.7", "6.0",
        "6.9", "5.6", "7.7", "6.3", "6.7", "7.2", "6.2", "6.1", "6.4", "7.2", "7.4", "7.9",
        "6.4", "6.3", "6.1", "7.7", "6.3", "6.4", "6.0", "6.9", "6.7", "6.9", "5.8", "6.8",
        "6.7", "6.7", "6.3", "6.5", "6.2", "5.9"
      ))
      val part1 = Partition.fullFrom(Array(
        "3.5", "3.0", "3.2", "3.1", "3.6", "3.9", "3.4", "3.4", "2.9", "3.1", "3.7", "3.4",
        "3.0", "3.0", "4.0", "4.4", "3.9", "3.5", "3.8", "3.8", "3.4", "3.7", "3.6", "3.3",
        "3.4", "3.0", "3.4", "3.5", "3.4", "3.2", "3.1", "3.4", "4.1", "4.2", "3.1", "3.2",
        "3.5", "3.1", "3.0", "3.4", "3.5", "2.3", "3.2", "3.5", "3.8", "3.0", "3.8", "3.2",
        "3.7", "3.3", "3.2", "3.2", "3.1", "2.3", "2.8", "2.8", "3.3", "2.4", "2.9", "2.7",
        "2.0", "3.0", "2.2", "2.9", "2.9", "3.1", "3.0", "2.7", "2.2", "2.5", "3.2", "2.8",
        "2.5", "2.8", "2.9", "3.0", "2.8", "3.0", "2.9", "2.6", "2.4", "2.4", "2.7", "2.7",
        "3.0", "3.4", "3.1", "2.3", "3.0", "2.5", "2.6", "3.0", "2.6", "2.3", "2.7", "3.0",
        "2.9", "2.9", "2.5", "2.8", "3.3", "2.7", "3.0", "2.9", "3.0", "3.0", "2.5", "2.9",
        "2.5", "3.6", "3.2", "2.7", "3.0", "2.5", "2.8", "3.2", "3.0", "3.8", "2.6", "2.2",
        "3.2", "2.8", "2.8", "2.7", "3.3", "3.2", "2.8", "3.0", "2.8", "3.0", "2.8", "3.8",
        "2.8", "2.8", "2.6", "3.0", "3.4", "3.1", "3.0", "3.1", "3.1", "3.1", "2.7", "3.2",
        "3.3", "3.0", "2.5", "3.0", "3.4", "3.0"
      ))

      (part0 * part1).equivClasses should contain theSameElementsAs expectedClasses
      (part0.stripped * part1.stripped).equivClasses should contain theSameElementsAs expectedClasses
    }
  }

  "Partition inversion" should {
    val sets = Seq(
      Set(0, 1),
      Set(2),
      Set(3),
      Set(4, 5)
    )
    val equivClasses = sets.map(set => Array.from(set)).toArray
    val tupleValueMap = TupleValueMap(6, 1f)
    tupleValueMap.put(0, 0)
    tupleValueMap.put(1, 0)
    tupleValueMap.put(2, 1)
    tupleValueMap.put(3, 2)
    tupleValueMap.put(4, 3)
    tupleValueMap.put(5, 3)
    tupleValueMap.trim()

    "convert from classes to tupleValueMap" in {
      val map = Partition.convertToTupleValueMap(equivClasses)
      map shouldEqual tupleValueMap
    }

    "convert from tupleValueMap to classes" in {
      val classes = Partition.convertFromTupleValueMap(tupleValueMap)
      classes shouldEqual equivClasses
    }
  }
}
