package com.github.codelionx.distod.types

import it.unimi.dsi.fastutil.ints.{Int2IntMap, Int2IntOpenHashMap}


object TupleValueMap {

  type TYPE = Int2IntMap

  type IMPL_TYPE = Int2IntOpenHashMap

  def empty: IMPL_TYPE = apply(0, 1f)

  def apply(expected: Int, f: Float): IMPL_TYPE = new Int2IntOpenHashMap(expected, f)
}
