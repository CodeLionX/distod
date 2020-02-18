package com.github.codelionx.distod.actors.worker

import com.github.codelionx.distod.discovery.CandidateValidation
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.types.OrderDependency.ConstantOrderDependency


class CheckSplitJob(
    override val candidateId: CandidateSet,
    override val candidates: CandidateSet
) extends CheckJob with CandidateValidation {

  override type T = CandidateSet

  val errorIds: Iterable[CandidateSet] = candidateId +: candidates.toSeq.map(c => candidateId - c)

  private var uncheckedCandidates: CandidateSet = candidates
  private var errors: Map[CandidateSet, Double] = Map.empty
  private var errorCompare: Option[Double] = None
  private var validCandidates: Seq[Int] = Seq.empty

  def receivedError(id: CandidateSet, error: Double): Unit = id match {
    case `candidateId` =>
      errorCompare = Some(error)
    case someId =>
      errors += someId -> error
  }

  override def performPossibleChecks(): Boolean = errorCompare match {
    case Some(errorCompare) =>
      val candidates = for {
        a <- uncheckedCandidates.unsorted
        context = candidateId - a
        errorContext <- errors.get(context)
      } yield a -> checkSplitCandidate(errorContext, errorCompare)

      validCandidates ++= candidates.filter(t => t._2).map(t => t._1)
      uncheckedCandidates --= candidates.map(t => t._1)
      allChecksFinished

    case None =>
      // no checks can be performed
      allChecksFinished
  }

  override def results(attributes: Seq[Int]): (Seq[ConstantOrderDependency], CandidateSet) =
    if (!allChecksFinished)
      throw new IllegalAccessException("Not all checks have been performed, too early access!")
    else {
      val validODs = validCandidates.map(a => ConstantOrderDependency(candidateId - a, a))
      val removedCandidates = {
        val validCandidateSet = CandidateSet.fromSpecific(validCandidates)
        if (validCandidateSet.nonEmpty)
          validCandidateSet union (CandidateSet.fromSpecific(attributes) diff candidateId)
        else
          CandidateSet.empty
      }
      (validODs, removedCandidates)
    }

  private def allChecksFinished: Boolean = uncheckedCandidates.isEmpty

}
