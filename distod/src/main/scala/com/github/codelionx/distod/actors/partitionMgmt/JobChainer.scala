package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.types.CandidateSet


object JobChainer {

  // 1 = store only the requested partition
  // 2 = store requested partition and its predecessors
  // 3 = store requested partition, its predecessors, and their predecessors
  // ...
  val storeDepth = 2

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
        val store = subkey.size >= key.size - storeDepth + 1

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
