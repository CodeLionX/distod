package com.github.codelionx.distod.actors.worker

import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.types.{CandidateSet, OrderDependency}
import com.github.codelionx.distod.types.OrderDependency.{ConstantOrderDependency, EquivalencyOrderDependency}

import scala.collection.mutable


object CandidateValidation {

  case class SplitCandidateValidationResult(
      validOds: Seq[OrderDependency],
      removedCandidates: CandidateSet
  )

  case class SwapCandidateValidationResult(
      validOds: Seq[OrderDependency],
      removedCandidates: Seq[(Int, Int)]
  )

  implicit class SortableStrippedPartition(val p: StrippedPartition) extends AnyVal {

    def sortEquivClassesBy(fullPartition: FullPartition): Array[IndexedSeq[Seq[Int]]] =
      fastSort(fullPartition)

    private def myFunctionalSort(fullPartition: FullPartition): IndexedSeq[IndexedSeq[Seq[Int]]] = {
      val indexLUT = fullPartition.toTupleValueMap

      p.equivClasses.map { clazz =>
        val subClazzes = mutable.SortedMap.empty[Int, mutable.Buffer[Int]]
        for (tuple <- clazz) {
          val index = indexLUT(tuple)
          val subClazz = subClazzes.getOrElseUpdate(index, mutable.Buffer.empty)
          subClazz += tuple
        }
        subClazzes.values.map(_.toSeq).toIndexedSeq
      }
    }

    private def fastSort(fullPartition: FullPartition): Array[IndexedSeq[Seq[Int]]] = {
      val indexLUT = fullPartition.toTupleValueMap
      val resultClasses = Array.ofDim[IndexedSeq[Seq[Int]]](p.numberClasses)

      val classes = p.equivClasses
      for (i <- 0 until classes.size) {
        val clazz = classes(i)
        val subClazzes = mutable.SortedMap.empty[Int, mutable.Builder[Int, Seq[Int]]]
        for (tuple <- clazz) {
          val index = indexLUT(tuple)
          val subClazz = subClazzes.getOrElseUpdate(index, Seq.newBuilder[Int])
          subClazz += tuple
        }
        resultClasses(i) = subClazzes.values.map(_.result()).toIndexedSeq
      }
      resultClasses
    }

    private def fastodSort(fullPartition: FullPartition): IndexedSeq[IndexedSeq[Seq[Int]]] = {
      val builder = Map.newBuilder[Int, Int]
      p.equivClasses.zipWithIndex.foreach { case (set, value) =>
        set.foreach(index =>
          builder.addOne(index, value)
        )
      }
      val indexLUT = builder.result()
      val resultClasses = Array.fill(p.numberClasses)(mutable.ArrayBuffer.empty[mutable.Builder[Int, Seq[Int]]])

      for (clazz <- fullPartition.equivClasses) {
        val seen = mutable.BitSet.empty
        for (tuple <- clazz) {
          indexLUT.get(tuple) match {
            case Some(index) =>
              if (!seen.contains(index)) {
                seen.add(index)
                resultClasses(index).addOne(Seq.newBuilder)
              }
              val currentResultClass = resultClasses(index)
              val lastIndex = currentResultClass.size - 1
              currentResultClass(lastIndex).addOne(tuple)
            case _ =>
          }
        }
      }
      resultClasses.map(clazz =>
        clazz.map(_.result()).toIndexedSeq
      )
    }
  }
}


trait CandidateValidation {

  import CandidateValidation._


  def checkSplitCandidates(
      candidateId: CandidateSet,
      splitCandidates: CandidateSet,
      allAttributes: Seq[Int],
      errors: Map[CandidateSet, Double]
  ): SplitCandidateValidationResult = {
    val errorCompare = errors(candidateId)
    val validConstantODs = for {
      a <- splitCandidates.unsorted
      fdContext = candidateId - a
      if errors(fdContext) == errorCompare // candidate fdContext: [] -> a holds
    } yield fdContext -> a

    val constantOds = validConstantODs.map {
      case (context, attribute) => ConstantOrderDependency(context, attribute)
    }.toSeq

    val removedCandidates = {
      val validCandidateSet = CandidateSet.fromSpecific(constantOds.map(_.constantAttribute))
      if (validCandidateSet.nonEmpty)
        validCandidateSet union (CandidateSet.fromSpecific(allAttributes) diff candidateId)
      else
        CandidateSet.empty
    }
    SplitCandidateValidationResult(
      validOds = constantOds,
      removedCandidates = removedCandidates
    )
  }

  def checkSwapCandidates(
      candidateId: CandidateSet,
      swapCandidates: Seq[(Int, Int)],
      singletonPartitions: Map[CandidateSet, FullPartition],
      candidatePartitions: Map[CandidateSet, StrippedPartition]
  ): SwapCandidateValidationResult = {
    val validCandidates = swapCandidates.flatMap { case (left, right) =>
      val context = candidateId - left - right
      val leftPartition = singletonPartitions(CandidateSet.from(left))
      val rightPartition = singletonPartitions(CandidateSet.from(right))

      val sortedContextClasses = candidatePartitions(context).sortEquivClassesBy(leftPartition)
      val rightTupleValueMapping = rightPartition.toTupleValueMap

      val swapFinder = findSwapFast(rightTupleValueMapping) _
      val results = sortedContextClasses.map(swapFinder)
      val (swap, reverseSwap) = results.reduce[(Boolean, Boolean)] { case ((s1, r1), (s2, r2)) => (s1 || s2, r1 || r2) }

      val normal = if (!swap)
        Seq(EquivalencyOrderDependency(context, left, right))
      else
        Seq.empty
      val reverse = if (!reverseSwap)
        Seq(EquivalencyOrderDependency(context, left, right, reverse = true))
      else
        Seq.empty
      normal ++ reverse
    }

    def isValid(candidate: (Int, Int)): Boolean = {
      val (left, right) = candidate
      validCandidates.exists(elem => elem.attribute1 == left && elem.attribute2 == right)
    }

    SwapCandidateValidationResult(
      validOds = validCandidates,
      removedCandidates = swapCandidates.filterNot(candidate => !isValid(candidate))
    )
  }

  private def findSwap(rightTupleValueMapping: Map[Int, Int])(sortedClass: IndexedSeq[Seq[Int]]): (Boolean, Boolean) = {
    val default = false -> false
    if (sortedClass.size < 2) {
      default
    } else {
      val combinations = sortedClass.sliding(2)
      combinations.foldLeft(default) { case ((formerSwap, formerReverseSwap), lists) =>
        val list1 = lists(0)
        val list2 = lists(1)

        val rightValues1 = list1.map(rightTupleValueMapping)
        val rightValues2 = list2.map(rightTupleValueMapping)
        val isSwap = rightValues1.max > rightValues2.min
        val isReverseSwap = rightValues2.max > rightValues1.min
        (formerSwap || isSwap) -> (formerReverseSwap || isReverseSwap)
      }
    }
  }

  private def findSwapFast(
      rightTupleValueMapping: Map[Int, Int]
  )(
      sortedClass: IndexedSeq[Seq[Int]]
  ): (Boolean, Boolean) = {
    val default = false -> false
    if (sortedClass.size < 2) {
      default
    } else {
      val combinations = sortedClass.sliding(2)
      // optimization: vars are faster here
      var (isSwap, isReverseSwap) = default
      for (lists <- combinations if !isSwap && !isReverseSwap) {
        val rightValues1 = lists(0).map(rightTupleValueMapping)
        val rightValues2 = lists(1).map(rightTupleValueMapping)
        isSwap = rightValues1.max > rightValues2.min
        isReverseSwap = rightValues2.max > rightValues1.min
      }
      (isSwap, isReverseSwap)
    }
  }
}
