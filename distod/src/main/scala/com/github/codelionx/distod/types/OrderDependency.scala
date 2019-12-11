package com.github.codelionx.distod.types


sealed trait OrderDependency {

  implicit class RichCandidateSet(set: CandidateSet) {

    def toSetString: String = s"{${set.mkString(", ")}}"
  }

  trait OrderDependencyWithAttributeNames

  def withAttributeNames(names: Seq[String]): OrderDependencyWithAttributeNames
}


object OrderDependency {
  final case class ConstantOrderDependency(context: CandidateSet, constantAttribute: Int) extends OrderDependency {

    override def toString: String = s"${context.toSetString}: [] ↦ $constantAttribute"

    override def withAttributeNames(names: Seq[String]): OrderDependencyWithAttributeNames =
      new OrderDependencyWithAttributeNames {
        override def toString: String = {
          val constantName = names(constantAttribute)
          val contextNames = context.map(names).mkString(", ")
          s"{$contextNames}: [] ↦ $constantName"
        }
      }
  }

  final case class EquivalencyOrderDependency(
      context: CandidateSet,
      attribute1: Int,
      attribute2: Int,
      reverse: Boolean = false
  ) extends OrderDependency {

    private val orderOfSecondAttribute =
      if (reverse)
        "↓"
      else
        "↑"

    override def toString: String = s"${context.toSetString}: $attribute1↑ ~ $attribute2$orderOfSecondAttribute"

    override def withAttributeNames(names: Seq[String]): OrderDependencyWithAttributeNames =
      new OrderDependencyWithAttributeNames {
        override def toString: String = {
          val attribute1Name = names(attribute1)
          val attribute2Name = names(attribute2)
          val contextNames = context.map(names).mkString(", ")
          s"{$contextNames}: $attribute1Name↑ ~ $attribute2Name$orderOfSecondAttribute"
        }
      }
  }
}


