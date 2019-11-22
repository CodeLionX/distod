lazy val akkaVersion = "2.6.0"
lazy val univocityVersion = "2.8.3"
lazy val clistVersion = "3.5.1"

lazy val scala212 = "2.12.10"
lazy val scala213 = "2.13.1"

scalaVersion := scala213
//crossScalaVersions := scala212 :: scala213 :: Nil

organization := "com.github.codelionx"
name := "distod"
version := "0.0.1"

libraryDependencies ++= Seq(
  // akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
//  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
//  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  // csv parsing
  "com.univocity" % "univocity-parsers" % univocityVersion,

  // logging
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  // test
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,

  // serialization
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion
//  "com.twitter" %% "chill-akka" % "0.9.3", // just for scala 2.12?
//  "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.2"
)

fork in run := true

// set main class for assembly
mainClass in assembly := Some("com.github.codelionx.distod.Main")

// skip tests during assembly
test in assembly := {}

// don't include logging configuration file
assemblyMergeStrategy in assembly := {
  case PathList("logback.xml") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
