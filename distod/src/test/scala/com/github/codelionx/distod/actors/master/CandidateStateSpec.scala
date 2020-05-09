package com.github.codelionx.distod.actors.master

import com.github.codelionx.distod.actors.master.CandidateState.{IncPrecondition, Prune}
import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class CandidateStateSpec extends AnyWordSpec with Matchers {

  "The initial candidate state" should {
    val initialState = CandidateState(CandidateSet.from(0))

    "have no split candidates" in {
      a[IllegalAccessException] shouldBe thrownBy(initialState.splitCandidates)
    }

    "have no swap candidates" in {
      a[IllegalAccessException] shouldBe thrownBy(initialState.swapCandidates)
    }

    "not have splits checked" in {
      initialState.splitChecked shouldBe false
    }

    "not have swap checked" in {
      initialState.swapChecked shouldBe false
    }

    "not be ready" in {
      initialState.isReadyToGenerate(JobType.Split) shouldBe false
      initialState.isReadyToGenerate(JobType.Swap) shouldBe false
    }

    "be ready after preconditions are met" in {
      val swapNotReady = initialState.updated(IncPrecondition(JobType.Swap))
      swapNotReady.isReadyToGenerate(JobType.Swap) shouldBe false

      val splitReady = initialState.updated(IncPrecondition(JobType.Split))
      splitReady.isReadyToGenerate(JobType.Split) shouldBe true
      splitReady.isReadyToGenerate(JobType.Swap) shouldBe false

      val splitAndSwapReady = splitReady.updated(IncPrecondition(JobType.Swap))
      splitAndSwapReady.isReadyToGenerate(JobType.Split) shouldBe true
      splitAndSwapReady.isReadyToGenerate(JobType.Swap) shouldBe true
    }

    "reject state updates" in {
      a[UnsupportedOperationException] shouldBe thrownBy {
        initialState.updated(CandidateState.NewSplitCandidates(CandidateSet.empty))
      }
    }
  }

  "A candidate state" should {
    val candidateState = CandidateState(CandidateSet.from(0, 1))

    "be ready if inc was called as often as we have attributes in its candidate id" in {
      val splitReadyCandidate = candidateState.updatedAll(Seq(IncPrecondition(JobType.Split), IncPrecondition(JobType.Split)))
      val swapReadyCandidate = splitReadyCandidate.updatedAll(Seq(IncPrecondition(JobType.Swap), IncPrecondition(JobType.Swap)))

      candidateState.isReadyToGenerate(JobType.Split) shouldBe false
      candidateState.isReadyToGenerate(JobType.Swap) shouldBe false

      splitReadyCandidate.isReadyToGenerate(JobType.Split) shouldBe true
      splitReadyCandidate.isReadyToGenerate(JobType.Swap) shouldBe false

      swapReadyCandidate.isReadyToGenerate(JobType.Swap) shouldBe true
    }

    "transition to ready state when candidates are added" in {
      val splitReady = candidateState
        .updatedAll(Seq(
          IncPrecondition(JobType.Split),
          IncPrecondition(JobType.Split),
          CandidateState.NewSplitCandidates(CandidateSet.empty)
        ))

      splitReady shouldBe a[SplitReadyCandidateState]

      val swapReady = splitReady
        .updatedAll(Seq(
          IncPrecondition(JobType.Swap),
          IncPrecondition(JobType.Swap),
          CandidateState.NewSwapCandidates(Seq.empty)
        ))

      swapReady shouldBe a[ReadyCandidateState]
    }

    "be pruned if both split and swaps candidate sets are empty and have been checked" in {
      val toBePrunedCandidate = FullyCheckedCandidateState(
        id = CandidateSet.empty,
        splitCandidates = CandidateSet.empty,
        swapCandidates = Seq.empty
      )
      val prunedCandidate =
        if (toBePrunedCandidate.shouldBePruned)
          toBePrunedCandidate.updated(Prune())
        else
          toBePrunedCandidate

      toBePrunedCandidate.isFullyChecked shouldBe true
      toBePrunedCandidate.isPruned shouldBe false

      prunedCandidate.isFullyChecked shouldBe true
      prunedCandidate.isPruned shouldBe true
    }
  }
}
