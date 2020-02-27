package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.actors.partitionMgmt.ComputePartitionProductJob.{PreviouslyComputedType, StrippedPartitionType}
import com.github.codelionx.distod.partitions.FullPartition
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
            loop(nextPred2, updatedJobs) :+ ComputeFromPredecessorsProduct(
              subkey,
              PreviouslyComputedType(nextPred1),
              PreviouslyComputedType(nextPred2),
              store
            )

          case foundPred :: Nil =>
            val nextPred :: _ = missingKeys
            loop(nextPred, jobs) :+ ComputeFromPredecessorsProduct(
              subkey,
              StrippedPartitionType(partitions(foundPred)),
              PreviouslyComputedType(nextPred),
              store
            )

          case _ =>
            val p1 :: p2 :: _ = foundKeys.map(partitions.apply).take(2)
            jobs :+ ComputeFromPredecessorsProduct(
              subkey,
              StrippedPartitionType(p1),
              StrippedPartitionType(p2),
              store
            )
        }
      }
    }

    loop(key, Seq.empty)
  }

  def calcLightJobChain(
      key: CandidateSet,
      partitions: CompactingPartitionMap
  ): Seq[ComputePartitionProductJob] = {
    val predecessorKeys = key.predecessors.toSeq.sortBy(_.sum)
    val foundKeys = predecessorKeys.filter(partitions.contains)
    val missingKeys = predecessorKeys.diff(foundKeys)
    val storePredecessor = storeDepth > 1

    foundKeys match {
      case Nil =>
        val singletons = findSingletonPartitions(key, partitions)
        Seq(ComputeFromSingletonsProduct(key, singletons, store = true))

      case foundPred :: Nil =>
        val nextPred :: _ = missingKeys
        val predecessorSingletons = findSingletonPartitions(nextPred, partitions)
        Seq(
          ComputeFromSingletonsProduct(nextPred, predecessorSingletons, store = storePredecessor),
          ComputeFromPredecessorsProduct(
            key,
            StrippedPartitionType(partitions(foundPred)),
            PreviouslyComputedType(nextPred),
            store = true
          )
        )

      case _ =>
        val p1 :: p2 :: _ = foundKeys.map(partitions.apply).take(2)
        Seq(
          ComputeFromPredecessorsProduct(
            key,
            StrippedPartitionType(p1),
            StrippedPartitionType(p2),
            store = true
          )
        )
    }
  }

  private def findSingletonPartitions(key: CandidateSet, partitions: CompactingPartitionMap): Seq[FullPartition] =
    key.toSeq.map(i => partitions.singletonPartition(CandidateSet.from(i)))
}
