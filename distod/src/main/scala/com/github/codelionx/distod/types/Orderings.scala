package com.github.codelionx.distod.types


@deprecated(message = "We transform `null`s to the empty string (mimics NULLS FIRST)")
object Orderings {

  val nullsFirst: Ordering[String] = Ordering.fromLessThan[String]{
    case (null, null) => true
    case (null, _) => true
    case (_, null) => false
    case (a, b) => a < b
  }

  val nullsLast: Ordering[String] = Ordering.fromLessThan[String]{
    case (null, null) => true
    case (null, _) => false
    case (_, null) => true
    case (a, b) => a < b
  }
}
