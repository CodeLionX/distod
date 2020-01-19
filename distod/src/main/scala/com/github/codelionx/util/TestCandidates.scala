package com.github.codelionx.util

import java.io.File

import com.github.codelionx.distod.actors.master.JobType
import com.github.codelionx.distod.actors.master.JobType.JobType
import com.github.codelionx.distod.partitions.{FullPartition, Partition, StrippedPartition}
import com.github.codelionx.distod.types.{CandidateSet, OrderDependency}

import scala.io.Source


object TestCandidates {

  lazy val testCandidates: Set[(CandidateSet, JobType)] = {
    val file = new File("data/test-candidates.txt")
    val source = Source.fromFile(file, "UTF-8")
    val lines = source.getLines()
    val candidates = lines.map(parseLine).toSet
    println(s"Size of compare set for candidates: ${candidates.size}")
    candidates
  }

  lazy val prunedCandidates: Set[CandidateSet] = {
    val file = new File("data/pruned-candidates.txt")
    val candidates = loadPruned(file)
    println(s"Size of prune compare set: ${candidates.size}")
    candidates
  }

  lazy val initialPrunedCandidates: Set[CandidateSet] = {
    val file = new File("data/initial-pruned-candidates.txt")
    val candidates = loadPruned(file)
    println(s"Size of initial prune compare set: ${candidates.size}")
    candidates
  }

  lazy val foundValidODs: Set[OrderDependency] = {
    val mapping = IndexedSeq("Year", "Quarter", "Month", "DayOfWeek", "FlightDate", "AirlineID", "Carrier", "TailNum", "FlightNum", "OriginAirportSeqID", "OriginCityMarketID", "Origin", "OriginCityName", "OriginStateFips", "OriginStateName", "OriginWac", "CRSDepTime", "DepDelay", "DepDelayMinutes", "DepDel15", "ArrTime", "ArrDelay", "ArrTimeBlk", "Diverted", "CRSElapsedTime", "AirTime", "Flights", "Distance")
    val reverseMapping = mapping.zipWithIndex.toMap
    ResultFileParsing
      .readAndParseDistodResults("data/found-results.txt")
      .toIndexedSeq
      .map(_.toOrderDependency(reverseMapping))
      .toSet
  }

  case class PartitionValidation(tpe: String, elems: Int, classes: Int) {

    def satisfies(p: Partition): Boolean = p match {
      case _: FullPartition if tpe == "StrippedPartition" => false
      case _: StrippedPartition if tpe == "FullPartition" => false
      case _ => p.numberElements == elems && p.numberClasses == classes
    }
  }

  lazy val partitions: Map[CandidateSet, PartitionValidation] = {
    val file = new File("data/partitions.csv")
    val source = Source.fromFile(file)
    val lines = source.getLines()
    val entries = lines.map { line =>
      val candidate :: tpe :: elems :: classes :: Nil = line.split(";").toList
      val strings = candidate.split(",")
      val key =
        if (strings.size == 1 && strings(0).trim == "") {
          CandidateSet.empty
        } else {
          val ids = strings.map(s => s.trim.toInt)
          CandidateSet.fromSpecific(ids)
        }
      key -> PartitionValidation(
        tpe,
        elems.trim.toInt,
        classes.trim.toInt
      )
    }
    entries.toMap
  }

  private def loadPruned(file: File): Set[CandidateSet] = {
    val source = Source.fromFile(file, "UTF-8")
    val lines = source.getLines()
    lines.map { line =>
      val ids = line.split(",").map(s => s.trim.toInt)
      CandidateSet.fromSpecific(ids)
    }.toSet
  }

  private def parseLine(line: String): (CandidateSet, JobType) = {
    val candidateString :: jobTypeString :: _ = line.split("-").toList

    val ids = candidateString
      .split(",")
      .filter(_.nonEmpty)
      .map(_.toInt)
    val id = CandidateSet.fromSpecific(ids)
    val jobType = jobTypeString match {
      case "JobType.Split" | "Split" => JobType.Split
      case "JobType.Swap" | "Swap" => JobType.Swap
    }
    id -> jobType
  }
}
