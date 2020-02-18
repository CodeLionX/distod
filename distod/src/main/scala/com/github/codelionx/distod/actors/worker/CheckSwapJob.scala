package com.github.codelionx.distod.actors.worker

import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.partitions.{FullPartition, Partition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.EquivalencyOrderDependency


class CheckSwapJob(
    override val candidateId: CandidateSet,
    override val candidates: Seq[(Int, Int)]
) extends CheckJob with CandidateValidation {

  override type T = Seq[(Int, Int)]

  val singletonPartitionKeys: Seq[CandidateSet] = {
    val distinctAttributes = candidates.flatMap { case (a, b) => Seq(a, b) }.distinct
    distinctAttributes.map(attribute => CandidateSet.from(attribute))
  }
  val partitionKeys: Seq[CandidateSet] = candidates.map { case (a, b) => candidateId - a - b }

  private var uncheckedCandidates: Map[CandidateSet, (Int, Int)] =
    candidates.map { case (a, b) =>
      val candidateContext = candidateId - a - b
      candidateContext -> (a, b)
    }.toMap
  private var singletonPartitions: Map[CandidateSet, FullPartition] = Map.empty
  private var candidatePartitions: Map[CandidateSet, StrippedPartition] = Map.empty
  private var validODs: Seq[EquivalencyOrderDependency] = Seq.empty

  def receivedPartition(key: CandidateSet, partition: Partition): Unit = partition match {
    case p: FullPartition =>
      singletonPartitions += (key -> p)
    case p: StrippedPartition =>
      candidatePartitions += key -> p
  }

  override def performPossibleChecks(): Boolean = {
    val results = for {
      (context, (a, b)) <- uncheckedCandidates
      contextPartition <- candidatePartitions.get(context)
      aPartition <- singletonPartitions.get(CandidateSet.from(a))
      bPartition <- singletonPartitions.get(CandidateSet.from(b))
    } yield context -> checkSwapCandidate(context, a, b, contextPartition, aPartition, bPartition)

    uncheckedCandidates --= results.keys
    candidatePartitions --= results.keys
    validODs ++= results.values.flatten
    allChecksFinished
  }

  override def results(attributes: Seq[Int]): (Seq[EquivalencyOrderDependency], Seq[(Int, Int)]) =
    if (!allChecksFinished)
      throw new IllegalAccessException("Not all checks have been performed, too early access!")
    else {
      val enrichedIsValid = isValid(validODs, _)
      val removedCandidates = candidates.filter(enrichedIsValid)
      (validODs, removedCandidates)
    }

  private def allChecksFinished: Boolean = uncheckedCandidates.isEmpty
}
