import java.io.File

import com.github.codelionx.distod.ResultFileParsing._
import org.scalatest.enablers.Aggregating
import org.scalatest.enablers.Aggregating._

val resultPath = "/home/sebastian/Projects/distod/data/results.txt"
val goldResultPath = "/home/sebastian/Projects/distod/data/gold/test-results.txt"

println(new File(resultPath).getAbsolutePath)

val ourResults = readAndParseDistodResults(resultPath)
val theirResults = readAndParseFastodResults(goldResultPath)
val aggregating: Aggregating[Seq[ODResult]] = implicitly[Aggregating[Seq[ODResult]]]
val isSameResult = aggregating.containsTheSameElementsAs(ourResults, theirResults)

if (!isSameResult) {
  def sorter(result: ODResult): String = result match {
    case ConstantODResult(context, _) => context.mkString("")
    case EquivalencyODResult(context, _, _, _) => context.mkString("")
  }

  println("=== Our results")
  println(ourResults.sortBy(sorter).mkString("\n"))
  println("=== Their results")
  println(theirResults.sortBy(sorter).mkString("\n"))
}

assert(isSameResult, "Did not contain the same alements as the gold standard")

