package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.types.CandidateSet


object JobChainer {

  def calcJobChain(
      key: CandidateSet,
      partitions: CompactingPartitionMap
  ): Seq[ComputePartitionProductJob] = {

    def loop(subkey: CandidateSet, jobs: Seq[ComputePartitionProductJob]): Seq[ComputePartitionProductJob] = {
      if(jobs.exists(_.key == subkey)) {
        jobs
      } else {
        val predecessorKeys = subkey.predecessors.toSeq.sortBy(_.sum)
        val foundKeys = predecessorKeys.filter(partitions.contains)
        val missingKeys = predecessorKeys.diff(foundKeys)
        val store = subkey.size >= key.size - 1 // store the requested partition and its predecessors (discard others)

        foundKeys match {
          case Nil =>
            val nextPred1 :: nextPred2 :: _ = missingKeys
            val updatedJobs = loop(nextPred1, jobs)
            loop(nextPred2, updatedJobs) :+ ComputePartitionProductJob(subkey, Left(nextPred1), Left(nextPred2), store)

          case foundPred :: Nil =>
            val nextPred :: _ = missingKeys
            loop(nextPred, jobs) :+ ComputePartitionProductJob(subkey, Right(partitions(foundPred)), Left(nextPred), store)

          case _ =>
            val p1 :: p2 :: _ = foundKeys.map(partitions.apply).take(2)
            jobs :+ ComputePartitionProductJob(subkey, Right(p1), Right(p2), store)
        }
      }
    }

    loop(key, Seq.empty)
  }
}
