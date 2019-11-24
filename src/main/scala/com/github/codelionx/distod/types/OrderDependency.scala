package com.github.codelionx.distod.types


sealed trait OrderDependency {

  implicit class RichCandidateSet(set: CandidateSet) {

    def toSetString: String = s"{${set.mkString(", ")}}"
  }
}


object OrderDependency {
  final case class ConstantOrderDependency(context: CandidateSet, constantAttribute: Int) extends OrderDependency {

    override def toString: String = s"${context.toSetString}: [] ↦ $constantAttribute"
  }

  final case class EquivalencyOrderDependency(
      context: CandidateSet,
      attribute1: Int,
      attribute2: Int,
      reverse: Boolean = false
  ) extends OrderDependency {

    override def toString: String = {
      val orderOfSecondAttribute =
        if (reverse)
          "↓"
        else
          "↑"
      s"${context.toSetString}: $attribute1↑ ~ $attribute2$orderOfSecondAttribute"
    }
  }
}


