package com.github.codelionx.distod.types

import it.unimi.dsi.fastutil.ints.{IntOpenHashSet, IntSet}

import scala.jdk.CollectionConverters._

object EquivClass {

  type TYPE = IntSet

  type IMPL_TYPE = IntOpenHashSet

  object Implicits {

    implicit class RichEquivClassImpl(val x: IMPL_TYPE) extends AnyVal {
      def trimSelf(): IMPL_TYPE = {
        x.trim()
        x
      }
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

    implicit class ConvertableSet(val set: Set[Int]) extends AnyVal {
      def toEquivClass: TYPE = from(set)
    }
  }

  import Implicits._

  def empty: IMPL_TYPE = apply(0, 1f)

  def apply(expected: Int, f: Float): IMPL_TYPE = new IntOpenHashSet(expected, f)

  def from(attributes: IterableOnce[Int]): IMPL_TYPE = new IntOpenHashSet(attributes.iterator.asJava, 1f).trimSelf()

}
