import java.io.{File, FileOutputStream, PrintWriter}

import com.github.codelionx.distod.ResultFileParsing._


val resultPath = "/home/sebastian/Documents/Projects/distod/data/results.txt"
val goldResultPath = "/home/sebastian/Documents/Projects/distod/data/gold/flight_500_28c-results.txt"

val ourResults = readAndParseDistodResults(resultPath)
val theirResults = readAndParseFastodResults(goldResultPath)

val ourOut = new PrintWriter(new FileOutputStream("our.txt"))
ourResults.sortBy(_.toString).foreach(result => ourOut.println(result))
ourOut.close()

val theirOut = new PrintWriter(new FileOutputStream("their.txt"))
theirResults.sortBy(_.toString).foreach(result => theirOut.println(result))
theirOut.close()

println(s"File location: ${new File("our.txt").getAbsolutePath}")
// files can then be compared with diff:
// `diff -y --suppress-common-lines our.txt their.txt`
