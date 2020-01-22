package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.CandidateState.{NewSplitCandidates, NewSwapCandidates}
import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.{Matchers, WordSpec}


class CandidateStateSpec extends WordSpec with Matchers {

  "The initial candidate state" should {
    val initialState = CandidateState(CandidateSet.from(0))

    "have no split candidates" in {
      initialState.splitCandidates shouldBe empty
    }

    "have no swap candidates" in {
      initialState.swapCandidates shouldBe empty
    }

    "not have splits checked" in {
      initialState.splitChecked shouldBe false
    }

    "not have swap checked" in {
      initialState.swapChecked shouldBe false
    }

    "not be ready" in {
      initialState.isReadyToCheck(JobType.Split) shouldBe false
      initialState.isReadyToCheck(JobType.Swap) shouldBe false
    }

    "be ready after preconditions are met" in {
      val swapNotReady = initialState.incPreconditions(JobType.Swap)
      swapNotReady.isReadyToCheck(JobType.Swap) shouldBe false

      val splitReady = initialState.incPreconditions(JobType.Split)
      splitReady.isReadyToCheck(JobType.Split) shouldBe true
      splitReady.isReadyToCheck(JobType.Swap) shouldBe false

      val splitAndSwapReady = splitReady.incPreconditions(JobType.Swap)
      splitAndSwapReady.isReadyToCheck(JobType.Split) shouldBe true
      splitAndSwapReady.isReadyToCheck(JobType.Swap) shouldBe true
    }

    "reject state updates" in {
      a[UnsupportedOperationException] shouldBe thrownBy{
        initialState.updated(CandidateState.NewSplitCandidates(CandidateSet.empty))
      }
    }
  }

  "A candidate state" should {
    val candidateState = CandidateState(CandidateSet.from(0, 1))

    "be ready if inc was called as often as we have attributes in its candidate id" in {
      val splitReadyCandidate = candidateState.incPreconditions(JobType.Split).incPreconditions(JobType.Split)
      val swapReadyCandidate = splitReadyCandidate.incPreconditions(JobType.Swap).incPreconditions(JobType.Swap)

      candidateState.isReadyToCheck(JobType.Split) shouldBe false
      candidateState.isReadyToCheck(JobType.Swap) shouldBe false

      splitReadyCandidate.isReadyToCheck(JobType.Split) shouldBe true
      splitReadyCandidate.isReadyToCheck(JobType.Swap) shouldBe false

      swapReadyCandidate.isReadyToCheck(JobType.Swap) shouldBe true
    }

    "transition to ready state when candidates are added" in {
      val splitReady = candidateState
        .incPreconditions(JobType.Split)
        .incPreconditions(JobType.Split)
        .updated(CandidateState.NewSplitCandidates(CandidateSet.empty))

      splitReady shouldBe a[SplitReadyCandidateState]

      val swapReady = splitReady
        .incPreconditions(JobType.Swap)
        .incPreconditions(JobType.Swap)
        .updated(CandidateState.NewSwapCandidates(Seq.empty))

      swapReady shouldBe a[ReadyCandidateState]
    }

    "be pruned if both split and swaps candidate sets are empty and have been checked" in {
      val toBePrunedCandidate = FullyCheckedCandidateState(
        id = CandidateSet.empty,
        splitCandidates = CandidateSet.empty,
        swapCandidates = Seq.empty
      )
      val prunedCandidate = toBePrunedCandidate.pruneIfConditionsAreMet

      toBePrunedCandidate.isFullyChecked shouldBe true
      toBePrunedCandidate.isPruned shouldBe false

      prunedCandidate.isFullyChecked shouldBe true
      prunedCandidate.isPruned shouldBe true
    }
  }
}
