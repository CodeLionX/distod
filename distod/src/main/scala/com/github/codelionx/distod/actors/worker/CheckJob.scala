package com.github.codelionx.distod.actors.worker

import com.github.codelionx.distod.types.{CandidateSet, OrderDependency}


trait CheckJob {

  type T

  def candidateId: CandidateSet

  def candidates: T

  def performPossibleChecks(): Boolean

  def results(attributes: Seq[Int]): (Seq[OrderDependency], T)
}
