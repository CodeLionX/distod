import java.io.File

import com.github.codelionx.distod.ResultFileParsing._
import org.scalatest.enablers.Aggregating
import org.scalatest.enablers.Aggregating._


val resultPath = "/home/sebastian/Documents/Projects/distod/data/results.txt"
val goldResultPath = "/home/sebastian/Documents/Projects/distod/data/gold/flight_500_28c-results.txt"

println(new File(resultPath).getAbsolutePath)

val ourResults = readAndParseDistodResults(resultPath)
val theirResults = readAndParseFastodResults(goldResultPath)
val aggregating: Aggregating[Seq[ODResult]] = implicitly[Aggregating[Seq[ODResult]]]
val isSameResult = aggregating.containsTheSameElementsAs(ourResults, theirResults)

if (!isSameResult) {
  println("=== Our results")
  println(ourResults.sortBy(_.toString).mkString("\n"))
  println("=== Their results")
  println(theirResults.sortBy(_.toString).mkString("\n"))
}

assert(isSameResult, "Did not contain the same elements as the gold standard")
