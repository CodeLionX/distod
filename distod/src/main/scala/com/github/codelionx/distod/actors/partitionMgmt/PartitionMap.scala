package com.github.codelionx.distod.actors.partitionMgmt

import com.github.codelionx.distod.partitions.{FullPartition, StrippedPartition}
import com.github.codelionx.distod.types.CandidateSet


object PartitionMap {

  def from(singletonPartitions: Map[CandidateSet, FullPartition]): PartitionMap =
    new PartitionMap(singletonPartitions, IndexedSeq.empty)

}


class PartitionMap private(
    singletonPartitions: Map[CandidateSet, FullPartition], // keys of size == 1
    levels: IndexedSeq[Map[CandidateSet, StrippedPartition]], // keys of size != 1
) {

  private def growLevels(size: Int): IndexedSeq[Map[CandidateSet, StrippedPartition]] = {
    val missing = size - (levels.size - 1)
    if(missing > 0) {
      levels ++ IndexedSeq.fill(missing){Map.empty[CandidateSet, StrippedPartition]}
    } else {
      levels
    }
  }

  def size: Int = levels.map(_.size).sum + singletonPartitions.size

  /**
   * Alias for `updated`
   *
   * @param kv the partition key/value pair.
   * @return A new partition map with the new binding added to this map.
   */
  @inline def +(kv: (CandidateSet, StrippedPartition)): PartitionMap = updated(kv._1, kv._2)

  /**
   * Creates a new partition map obtained by updating this map with a given key/value pair.
   *
   * @param    key   the candidate
   * @param    value the partition
   * @return A new partition map with the new key/value mapping added to this map.
   */
  def updated(key: CandidateSet, value: StrippedPartition): PartitionMap = {
    val newLevels = growLevels(key.size)
    val newMap = newLevels(key.size).updated(key, value)
    val updatedLevels = newLevels.updated(key.size, newMap)
    new PartitionMap(singletonPartitions, updatedLevels)
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
      case 1 => singletonPartitions.get(key).map(_.stripped)
      case i if i < levels.size => levels(key.size).get(key)
      case _ => None
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
   * Frees up the space of a complete level by discarding all partitions corresponding to this level.
   *
   * @param level id of the level (key size) != 1
   */
  def removeLevel(level: Int): PartitionMap = {
    if(level == 1) {
      throw new IllegalArgumentException("Can not discard all singleton partitions!")
    }
    val updatedLevels = levels.updated(level, Map.empty)
    new PartitionMap(singletonPartitions, updatedLevels)
  }

}
