package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.CandidateState.{NewSplitCandidates, NewSwapCandidates}
import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.{Matchers, WordSpec}


class CandidateStateSpec extends WordSpec with Matchers {

  "An empty candidate state" should {
    val emptyState = CandidateState(CandidateSet.from(0))

    "have no split candidates" in {
      emptyState.splitCandidates shouldBe empty
    }

    "have no swap candidates" in {
      emptyState.swapCandidates shouldBe empty
    }

    "not have splits checked" in {
      emptyState.splitChecked shouldBe false
    }

    "not have swap checked" in {
      emptyState.swapChecked shouldBe false
    }

    "not be ready" in {
      emptyState.isReadyToCheck(JobType.Split) shouldBe false
      emptyState.isReadyToCheck(JobType.Swap) shouldBe false
    }

    "accept new split candidates" in {
      val newSplitCandidates = CandidateSet.from(0, 1, 2, 3)
      val state = emptyState.updated(NewSplitCandidates(newSplitCandidates))
      state.splitCandidates shouldEqual newSplitCandidates
    }

    "accept new swap candidates" in {
      val newSwapCandidates = Seq(0 -> 1)
      val state = CandidateState(CandidateSet.from(0, 1))
      val newState = state.updated(NewSwapCandidates(newSwapCandidates))
      newState.swapCandidates shouldEqual newSwapCandidates
    }

  }

  "A candidate state" should {
    val startCandidate = CandidateState(CandidateSet.from(0, 1))

    "be ready if inc was called as often as we have attributes in its candidate id" in {
      val splitReadyCandidate = startCandidate.incSplitPreconditions.incSplitPreconditions
      val swapReadyCandidate = splitReadyCandidate.incSwapPreconditions.incSwapPreconditions

      startCandidate.isReadyToCheck(JobType.Split) shouldBe false
      startCandidate.isReadyToCheck(JobType.Swap) shouldBe false

      splitReadyCandidate.isReadyToCheck(JobType.Split) shouldBe true
      splitReadyCandidate.isReadyToCheck(JobType.Swap) shouldBe false

      swapReadyCandidate.isReadyToCheck(JobType.Swap) shouldBe true
    }

    "be pruned if both split and swaps candidate sets are empty and have been checked" in {
      val prunedCandidate = startCandidate.updatedAll(Seq(
        CandidateState.SplitChecked(CandidateSet.empty),
        CandidateState.SwapChecked(Seq.empty)
      ))

      startCandidate.isPruned shouldBe false
      prunedCandidate.isFullyChecked shouldBe true
      prunedCandidate.isPruned shouldBe true
    }
  }
}
