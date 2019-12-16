package com.github.codelionx.distod.types

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import com.github.codelionx.distod.Serialization.{CborSerializable, JsonSerializable}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[OrderDependency.ConstantOrderDependency]),
  new JsonSubTypes.Type(value = classOf[OrderDependency.EquivalencyOrderDependency]),
))
sealed trait OrderDependency extends CborSerializable with JsonSerializable {

  implicit class RichCandidateSet(set: CandidateSet) {

    def toSetString: String = s"{${set.mkString(", ")}}"
  }

  trait OrderDependencyWithAttributeNames

  def withAttributeNames(names: Seq[String]): OrderDependencyWithAttributeNames
}


object OrderDependency {

  @JsonTypeName("ConstantOrderDependency")
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

  @JsonTypeName("EquivalencyOrderDependency")
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


