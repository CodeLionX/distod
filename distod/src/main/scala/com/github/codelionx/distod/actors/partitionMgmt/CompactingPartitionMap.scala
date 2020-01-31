package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet
import com.github.codelionx.distod.Settings.PartitionCompactionSettings
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable


object CompactingPartitionMap {

  def apply(settings: PartitionCompactionSettings): CompactingPartitionMapBuilder =
    new CompactingPartitionMapBuilder(settings)

  class CompactingPartitionMapBuilder private[CompactingPartitionMap] (settings: PartitionCompactionSettings) {

    def from(singletonPartitions: Map[CandidateSet, FullPartition]): CompactingPartitionMap =
      new CompactingPartitionMap(settings, singletonPartitions)
  }
}


class CompactingPartitionMap private(
    compactionSettings: PartitionCompactionSettings,
    initialSingletonPartitions: Map[CandidateSet, FullPartition]
) {

  // keys of size == 1
  private val singletonPartitions: Map[CandidateSet, FullPartition] = initialSingletonPartitions
  // keys of size != 1
  private var levels: IndexedSeq[mutable.Map[CandidateSet, StrippedPartition]] = IndexedSeq.empty
  private var usage: IndexedSeq[mutable.Map[CandidateSet, Int]] = IndexedSeq.empty
  private val accessCounter: mutable.IndexedBuffer[Long] = mutable.IndexedBuffer.empty

  private val log: Logger = LoggerFactory.getLogger(classOf[CompactingPartitionMap])

  private def growLevels(size: Int): Unit = {
    val missing = size - (levels.size - 1)
    if (missing > 0) {
      levels ++= IndexedSeq.fill(missing) {
        mutable.Map.empty[CandidateSet, StrippedPartition]
      }
      usage ++= IndexedSeq.fill(missing) {
        mutable.Map.empty[CandidateSet, Int]
      }
      accessCounter ++= IndexedSeq.fill(missing)(0)
      if(compactionSettings.enabled && size > 3) {
        log.info("Triggering auto-compaction based on level growth to level {}", size)
        compact()
      }
    }
  }

  private def updateUsage(key: CandidateSet): Unit = {
    usage(key.size).updateWith(key){
      case None => Some(1)
      case Some(v) => Some(v + 1)
    }
    accessCounter(key.size) = accessCounter(key.size) + 1
    if(accessCounter(key.size) >= compactionSettings.levelAccessThreshold) {
      log.info("Triggering auto-compaction based on access counter threshold")
      compact()
    }
  }

  private def logStatistics(message: String, log: (String, Any) => Unit): Unit = {
    val statistics = levels.indices.map(i =>
      s"level $i: level ${levels(i).size}, usage ${usage(i).size}, accesses: ${accessCounter(i)}"
    )
    log(s"$message Partition Statistics: {}", statistics.mkString("\n", "\n", ""))
  }

  def size: Int = levels.map(_.size).sum + singletonPartitions.size

  /**
   * Alias for `updated`
   *
   * @param kv the partition key/value pair.
   * @return A new partition map with the new binding added to this map.
   */
  @inline def +(kv: (CandidateSet, StrippedPartition)): Unit = update(kv._1, kv._2)

  /**
   * Updates this map with a given key/value pair.
   *
   * @param    key   the candidate
   * @param    value the partition
   */
  def update(key: CandidateSet, value: StrippedPartition): Unit = {
    growLevels(key.size)
    levels(key.size).update(key, value)
    if(compactionSettings.enabled)
      updateUsage(key)
  }

  /**
   * Optionally returns the singleton partition (full partition) associated with a key.
   *
   * @param  key the candidate
   * @return an option value containing the value associated with `key` in this map,
   *         or `None` if none exists or the key is invalid (size != 1).
   */
  def getSingletonPartition(key: CandidateSet): Option[FullPartition] =
    if (key.size != 1)
      None
    else
      singletonPartitions.get(key)

  /**
   * Optionally returns the stripped partition associated with a key.
   *
   * @param  key the candidate
   * @return an option value containing the value associated with `key` in this map,
   *         or `None` if none exists.
   */
  def get(key: CandidateSet): Option[StrippedPartition] =
    key.size match {
      case 1 =>
        singletonPartitions.get(key).map(_.stripped)
      case i if i < levels.size =>
        val result = levels(key.size).get(key)
        if(compactionSettings.enabled && result.isDefined) {
          updateUsage(key)
        }
        result
      case _ =>
        None
    }

  /**
   * Returns the value associated with a key, or a default value if the key is not contained in the partition map.
   *
   * @param   key     the candidate
   * @param   default a computation that yields a default stripped partition in case no binding for `key` is
   *                  found in the map.
   * @return the partition associated with `key` if it exists,
   *         otherwise the result of the `default` computation.
   */
  def getOrElse(key: CandidateSet, default: => StrippedPartition): StrippedPartition = get(key) match {
    case Some(v) => v
    case None => default
  }

  /**
   * Retrieves the partition which is associated with the given key. This method throws a `NoSuchElementException`
   * if there is no mapping from the given key to a partition.
   *
   * @param  key the candidate
   * @return the value associated with the given key, or the result of the
   *         `default` method, if none exists.
   */
  @throws[NoSuchElementException]
  def apply(key: CandidateSet): StrippedPartition = get(key) match {
    case None => throw new NoSuchElementException("candidate key not found: " + key)
    case Some(value) => value
  }

  /**
   * Tests whether this partition map contains a binding for a key.
   *
   * @param key the candidate
   * @return `true` if there is a binding for `key` in this partition map, `false` otherwise.
   */
  def contains(key: CandidateSet): Boolean = get(key).isDefined

  /**
   * Compacts this partition map by discarding unused partitions (excluding singleton partitions with key size = 1)
   */
  def compact(): Unit = {
    if(log.isTraceEnabled) {
      logStatistics("Before compaction:", log.trace)
    }
    var removed = 0
    for(i <- levels.indices) {
      val level = levels(i)
      val use = usage(i)
      if(use.size < level.size) {
        val removableKeys = level.keySet.toSet -- use.keySet
        level.subtractAll(removableKeys)
        removed += removableKeys.size
      }
    }
    usage.foreach(usageLevel => usageLevel.clear())
    for(i <- accessCounter.indices) {
      accessCounter(i) = 0
    }
    log.info("Partition map compaction removed {} partitions", removed)
    if(log.isTraceEnabled) {
      logStatistics("After compaction:", log.trace)
    }
  }

  /**
   * Frees up the memory that is used by temporary partitions. This does not include the singleton partitions.
   * This method also resets all partition usage statistics.
   */
  def clear(): Unit = {
    // skip empty partition to prevent early deletion
    levels.tail.foreach(_.clear())
    usage.foreach(_.clear())
    for(i <- accessCounter.indices) {
      accessCounter(i) = 0
    }
  }

  /**
   * Frees up the space of a complete level by discarding all partitions corresponding to this level.
   *
   * @param level id of the level (key size) != 1
   */
  def removeLevel(level: Int): Unit = {
    if (level == 1) {
      throw new IllegalArgumentException("Can not discard all singleton partitions!")
    }
    levels = levels.updated(level, mutable.Map.empty[CandidateSet, StrippedPartition])
  }

}
