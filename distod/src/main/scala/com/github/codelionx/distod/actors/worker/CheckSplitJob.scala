package com.github.codelionx.distod.actors.worker

import com.github.codelionx.distod.actors.worker.CheckSplitJob.CheckResult
import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.partitions.{FullPartition, Partition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.ConstantOrderDependency


object CheckSplitJob {
  private[CheckSplitJob] final case class CheckResult(id: Int, isValid: Boolean, isPruned: Boolean = false)
}


class CheckSplitJob(
    override val candidateId: CandidateSet,
    override val candidates: CandidateSet,
    private val interestingnessThreshold: Option[Long]
) extends CheckJob with CandidateValidation {

  override type T = CandidateSet

  val errorIds: Iterable[CandidateSet] = candidateId +: candidates.toSeq.map(c => candidateId - c)
  val partitionIds: Iterable[CandidateSet] = interestingnessThreshold match {
    case None => Iterable.empty
    case Some(_) => candidates.unsorted.map(a => candidateId - a)
  }

  private var uncheckedCandidates: CandidateSet = candidates
  private var errors: Map[CandidateSet, Double] = Map.empty
  private var scores: Map[CandidateSet, Long] = Map.empty
  private var errorCompare: Option[Double] = None
  private var validCandidates: Seq[Int] = Seq.empty
  private var prunedCandidates: Seq[Int] = Seq.empty

  def receivedError(id: CandidateSet, error: Double): Unit = id match {
    case `candidateId` =>
      errorCompare = Some(error)
    case someId =>
      errors += someId -> error
  }

  override def receivedPartition(id: CandidateSet, partition: Partition): Unit = {
    val p = partition match {
      case p: StrippedPartition => p
      case p: FullPartition => p.stripped
    }
    scores += id -> calculateInterestingnessScore(id, p)
  }

  override def performPossibleChecks(): Boolean = {
    val checkedCandidates = (errorCompare, interestingnessThreshold) match {
      case (Some(errorCompare), None) =>
        for {
          a <- uncheckedCandidates.unsorted
          context = candidateId - a
          errorContext <- errors.get(context)
        } yield CheckResult(a, isValid = checkSplitCandidate(errorContext, errorCompare))

      case (Some(errorCompare), Some(threshold)) =>
        for {
          a <- uncheckedCandidates.unsorted
          context = candidateId - a
          errorContext <- errors.get(context)
          score <- scores.get(context)
        } yield {
          // threshold set && score is too small --> not valid
          if (score <= threshold)
            CheckResult(a, isValid = false, isPruned = true)
          else
            CheckResult(a, isValid = checkSplitCandidate(errorContext, errorCompare))
        }

      case (None, _) =>
        // no checks can be performed
        Seq.empty
    }

    validCandidates ++= checkedCandidates.filter(_.isValid).map(_.id)
    prunedCandidates ++= checkedCandidates.filter(_.isPruned).map(_.id)
    uncheckedCandidates --= checkedCandidates.map(_.id)
    allChecksFinished
  }

  override def results(attributes: Seq[Int]): (Seq[ConstantOrderDependency], CandidateSet) =
    if (!allChecksFinished)
      throw new IllegalAccessException("Not all checks have been performed, too early access!")
    else {
      val validODs = validCandidates.map(a => ConstantOrderDependency(candidateId - a, a))
      val removedCandidates = {
        val validCandidateSet = CandidateSet.fromSpecific(validCandidates ++ prunedCandidates)
        if (validCandidateSet.nonEmpty)
          validCandidateSet union (CandidateSet.fromSpecific(attributes) diff candidateId)
        else
          CandidateSet.empty
      }
      (validODs, removedCandidates)
    }

  private def allChecksFinished: Boolean = uncheckedCandidates.isEmpty

}
