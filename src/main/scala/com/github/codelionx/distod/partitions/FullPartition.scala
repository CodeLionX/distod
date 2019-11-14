package com.github.codelionx.distod.partitions

import scala.collection.SortedMap


/**
 * A partition with all equivalence classes.
 *
 * @param numberElements number of all elements in all equivalence classes
 * @param numberClasses number of equivalence classes
 * @param equivClasses
 */
case class FullPartition private[partitions](
                                              numberElements: Int,
                                              numberClasses: Int,
                                              equivClasses: SortedMap[Value, Set[Index]]
                                            ) extends Partition