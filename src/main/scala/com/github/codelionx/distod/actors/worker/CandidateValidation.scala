package com.github.codelionx.distod.actors.worker

import com.github.codelionx.distod.actors.worker.CandidateValidation.{SplitCandidateValidationResult, SwapCandidateValidationResult}
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
}


trait CandidateValidation {

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
      val contextPartition = candidatePartitions(context)
      val leftPartition = singletonPartitions(CandidateSet.from(left))
      val rightPartition = singletonPartitions(CandidateSet.from(right))

      val leftTupleValueMapping = leftPartition.toTupleValueMap
      val rightTupleValueMapping = rightPartition.toTupleValueMap

      val sortedContextClasses = contextPartition.equivClasses.map { clazz =>
        val subClazzes = mutable.Map.empty[Int, mutable.Buffer[Int]]
        for (tuple <- clazz) {
          val index = leftTupleValueMapping(tuple)
          val subClazz = subClazzes.getOrElseUpdate(index, mutable.Buffer.empty)
          subClazz += tuple
        }
        subClazzes.values.map(_.toSeq).toIndexedSeq
      }

      var swap = false
      var reverseSwap = false
      for (sortedClass <- sortedContextClasses if !swap && !reverseSwap) {
        for (i <- 0 until sortedClass.size - 1 if !swap && !reverseSwap) {
          val list1 = sortedClass(i)
          val list2 = sortedClass(i + 1)

          val rightValues1 = list1.map(rightTupleValueMapping)
          val rightValues2 = list2.map(rightTupleValueMapping)
          if (rightValues1.max > rightValues2.min) {
            swap = true
          }
          if (rightValues2.max > rightValues1.min) {
            reverseSwap = true
          }
        }
      }

      (swap, reverseSwap) match {
        case (false, false) =>
          Seq(
            EquivalencyOrderDependency(context, left, right),
            EquivalencyOrderDependency(context, left, right, reverse = true)
          )
        case (false, true) =>
          Seq(EquivalencyOrderDependency(context, left, right))
        case (true, false) =>
          Seq(EquivalencyOrderDependency(context, left, right, reverse = true))
        case _ =>
          Seq.empty
      }
    }

    SwapCandidateValidationResult(
      validOds = validCandidates,
      removedCandidates = swapCandidates.filterNot { case (left, right) =>
        !validCandidates.exists(elem => elem.attribute1 == left && elem.attribute2 == right)
      }
    )
  }
}
