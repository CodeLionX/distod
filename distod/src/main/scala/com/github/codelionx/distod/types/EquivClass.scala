package com.github.codelionx.distod.types

import it.unimi.dsi.fastutil.ints.{IntAVLTreeSet, IntSet}

object EquivClass {

  type TYPE = IntSet

  type IMPL_TYPE = IntAVLTreeSet // IntOpenHashSet

  object Implicits {

    implicit class RichEquivClassImpl(val set: IMPL_TYPE) extends AnyVal {

      /** calls `trim()` if possible and returns itself */
      def trimSelf(): IMPL_TYPE = set

      /** Preserves type information */
      def copy(): IMPL_TYPE = set.clone().asInstanceOf[IMPL_TYPE]
    }

    implicit class RichEquivClass(val set: TYPE) extends AnyVal {

      def filter(pred: Int => Boolean): Set[Int] = {
        var target = Set.empty[Int]
        val iter = set.iterator
        while (iter.hasNext) {
          val value = iter.nextInt
          if (pred(value))
            target += value
        }
        target
      }
    }

    implicit class ConvertibleSet(val set: Set[Int]) extends AnyVal {
      def toEquivClass: TYPE = from(set)
    }
  }

  import Implicits._

  def empty: IMPL_TYPE = apply(0, 1f)

  def apply(expected: Int, f: Float): IMPL_TYPE = new IntAVLTreeSet()

  def from(attributes: IterableOnce[Int]): IMPL_TYPE = {
    import scala.jdk.CollectionConverters._
    new IntAVLTreeSet(attributes.iterator.asJava).trimSelf()
  }

}
