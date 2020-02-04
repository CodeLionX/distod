package com.github.codelionx.util.largeMap

import com.github.codelionx.distod.actors.master.CandidateState
import com.github.codelionx.distod.types.CandidateSet
import org.scalatest.matchers.should.Matchers


object StateTestingFixtures extends Matchers {

  implicit class ExpectingCandidateSet(val cs: CandidateSet) extends AnyVal {

    def stateRepr: CandidateState = CandidateState(cs)

    def tuple: (CandidateSet, CandidateState) = cs -> stateRepr
  }

  val csEmpty: CandidateSet = CandidateSet.empty
  val cs0: CandidateSet = CandidateSet.from(0)
  val cs1: CandidateSet = CandidateSet.from(1)
  val cs01: CandidateSet = cs0 + 1
  val cs012: CandidateSet = cs01 + 2
  val cs013: CandidateSet = cs01 + 3
}
