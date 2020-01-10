package com.github.codelionx.distod

import java.io.{BufferedReader, FileReader}

import com.github.codelionx.util.TryWithResource._

import scala.collection.immutable.Set
import scala.jdk.StreamConverters._


object ResultFileParsing {

  object ODType {
    sealed trait Type
    case object Constant extends Type
    case object Equivalency extends Type
  }

  sealed trait ODResult {

    def context: Set[String]
  }
  final case class ConstantODResult(context: Set[String], constantAttribute: String) extends ODResult {
    override def toString: String = s"ConstantOD: {${context.toSeq.sorted}}: ${constantAttribute}"
  }
  final case class EquivalencyODResult(
      context: Set[String],
      attribute1: String,
      attribute2: String,
      reverse: Boolean = false
  ) extends ODResult {
    override def toString: String =
      s"EquivalencyOD: {${context.toSeq.sorted}}: $attribute1 ~ $attribute2, reverse:${reverse}"
  }

  def readAndParseDistodResults(path: String): Seq[ODResult] =
    withResource(new BufferedReader(new FileReader(path))) { reader =>
      val lines = reader.lines().toScala(LazyList)
      DistodResultParser.parseAll(lines)
    }

  def readAndParseFastodResults(path: String): Seq[ODResult] =
    withResource(new BufferedReader(new FileReader(path))) { reader =>
      val lines = reader.lines().toScala(LazyList)
      FastodResultParser.parseAll(lines)
    }

  trait ResultParser {

    protected val separator = ":"
    protected val compatibilitySymbol = "~"
    protected val constantSymbol = "[] ↦"

    protected def parseContextAttributes(context: String): Set[String] = {
      val contextString = context.substring(context.indexOf("{") + 1, context.indexOf("}"))
      val contextAttributes = contextString.split(",").map(_.trim).toSet
      contextAttributes match {
        case s if s.forall(_.isEmpty) => Set.empty
        case s => s
      }
    }

    protected def parseConstantAttribute(dependencyPart: String): String =
      dependencyPart.replace(constantSymbol, "").trim()

    def parseAll(lines: Seq[String]): Seq[ODResult] = lines.map(parseResult)

    def parseResult(s: String): ODResult
  }

  object DistodResultParser extends ResultParser {

    private val asc = "↑"
    private val desc = "↓"

    override def parseResult(s: String): ODResult = {
      val cuttingPoint = s.indexOf(separator)
      val part1 = s.substring(0, cuttingPoint).trim()
      val part2 = s.substring(cuttingPoint + 1, s.length).trim()

      val tpe = part2 match {
        case part if part.contains(constantSymbol) =>
          ODType.Constant
        case part if part.contains(compatibilitySymbol) =>
          ODType.Equivalency
        case _ =>
          throw new IllegalArgumentException(s + " is not a valid OD!")
      }

      // part1 is the context attribute set
      val contextAttributes = parseContextAttributes(part1)

      // part2 parsing depends on type
      tpe match {
        case ODType.Constant =>
          val attribute = parseConstantAttribute(part2)
          ConstantODResult(contextAttributes, attribute)

        case ODType.Equivalency =>
          val rawAttributes = part2.split(compatibilitySymbol)
          val isReverse = rawAttributes(1).contains(desc)
          val attributes = rawAttributes.map(a =>
            a.replace(asc, "").replace(desc, "").trim()
          )
          EquivalencyODResult(contextAttributes, attributes(0), attributes(1), reverse = isReverse)
      }
    }
  }

  object FastodResultParser extends ResultParser {

    override protected val constantSymbol = "[] ->"

    override def parseResult(s: String): ODResult = {
      val parts = s.split(separator).map(_.trim())
      val tpeString = parts(0)
      val contextString = parts(1)
      val dependencyString = parts(2)

      // parse context
      val contextAttributes = parseContextAttributes(contextString)

      // parse dependency depending on the type
      tpeString match {
        case "FD" =>
          val attribute = parseConstantAttribute(dependencyString)
          ConstantODResult(contextAttributes, attribute)

        case "REG-OD" =>
          val attributes = dependencyString.split(compatibilitySymbol).map(_.trim())
          EquivalencyODResult(contextAttributes, attributes(0), attributes(1))

        case "BID-OD" =>
          val attributes = dependencyString.split(compatibilitySymbol).map(_.trim())
          EquivalencyODResult(contextAttributes, attributes(0), attributes(1), reverse = true)
      }
    }
  }
}
