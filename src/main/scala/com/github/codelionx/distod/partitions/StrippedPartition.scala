package com.github.codelionx.distod.partitions

import scala.collection.SortedMap


/**
 * A stripped partition with equivalence classes of size one removed.
 *
 * @param numberElements number of all elements in all remaining equivalence classes
 * @param numberClasses number of remaining equivalence classes
 * @param equivClasses
 */
case class StrippedPartition private[partitions](
                                                  numberElements: Int,
                                                  numberClasses: Int,
                                                  equivClasses: SortedMap[Value, Set[Index]]
                                                ) extends Partition
