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

  implicit class SortableStrippedPartition(p: StrippedPartition) {

    def sortEquivClassesBy(fullPartition: FullPartition): IndexedSeq[IndexedSeq[Seq[Int]]] = {
      val indexLUT = fullPartition.toTupleValueMap

      p.equivClasses.map { clazz =>
        val subClazzes = mutable.Map.empty[Int, mutable.Buffer[Int]]
        for (tuple <- clazz) {
          val index = indexLUT(tuple)
          val subClazz = subClazzes.getOrElseUpdate(index, mutable.Buffer.empty)
          subClazz += tuple
        }
        subClazzes.values.map(_.toSeq).toIndexedSeq
      }
    }
  }

  private[CandidateValidation] object SwapTestResult {
    sealed trait Type
    case object NoSwap extends Type
    case object NoReverseSwap extends Type
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

      val testResults = sortedContextClasses.flatMap(sortedClass =>
        sortedClass.sliding(2).flatMap { lists =>
          val list1 = lists(0)
          val list2 = lists(1)

          val rightValues1 = list1.map(rightTupleValueMapping)
          val rightValues2 = list2.map(rightTupleValueMapping)
          val hasNoSwap = for {
            x <- List(SwapTestResult.NoSwap) if !(rightValues1.max > rightValues2.min)
          } yield x
          val hasNoReverseSwap = for {
            x <- List(SwapTestResult.NoReverseSwap) if !(rightValues2.max > rightValues1.min)
          } yield x
          hasNoSwap ++ hasNoReverseSwap
        }
      )

      testResults.flatMap {
        case SwapTestResult.NoSwap => Seq(EquivalencyOrderDependency(context, left, right))
        case SwapTestResult.NoReverseSwap => Seq(EquivalencyOrderDependency(context, left, right, reverse = true))
      }
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
}
