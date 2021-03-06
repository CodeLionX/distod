lazy val akkaVersion = "2.6.3"
lazy val univocityVersion = "2.8.3"
lazy val clistVersion = "3.5.1"

lazy val scala212 = "2.12.10"
lazy val scala213 = "2.13.1"

enablePlugins(GitVersioning)

ThisBuild / scalaVersion := scala213
//crossScalaVersions := scala212 :: scala213 :: Nil

ThisBuild / organization := "com.github.codelionx"
//ThisBuild / version := "0.0.1"
ThisBuild / git.baseVersion := "1.0.6"

ThisBuild / fork in run := true

lazy val root = (project in file("."))
  .aggregate(distod, benchmarking)

lazy val distod = (project in file("distod"))
  .settings(
    name := "distod",
    libraryDependencies ++= Seq(
      // akka
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
      //  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,

      // fastutil
      "it.unimi.dsi" % "fastutil" % "8.3.0",

      // csv parsing
      "com.univocity" % "univocity-parsers" % univocityVersion,

      // logging
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",

      // test
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
      "org.scalactic" %% "scalactic" % "3.1.0",
      "org.scalatest" %% "scalatest" % "3.1.0" % Test,

      // serialization
      "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion
      //  "com.twitter" %% "chill-akka" % "0.9.3", // just for scala 2.12?
      //  "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.2"
    ),

    // set java options for DISTOD
    javaOptions in run ++= Seq("-Xms2G", "-Xmx2G", "-XX:+UseG1GC", "-Dfile.encoding=UTF-8"),

    // test configuration
    parallelExecution in Test := true,
    logBuffered in Test := false,

    // skip tests during assembly
    test in assembly := {},

    // set main class for assembly
    mainClass in assembly := Some("com.github.codelionx.distod.Main"),

    assemblyMergeStrategy in assembly := {
      // discard JDK11 module infos from libs (not required for assembly and JDK8)
      case "module-info.class" => MergeStrategy.discard
      // discard logging configuration (set during deployment)
      case PathList("logback.xml") => MergeStrategy.discard
      // discard follower configuration (only used for local testing)
      case PathList("application-follower.conf") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
  )
  .settings(VersionGenerator.settings)

// documentation at https://github.com/ktoso/sbt-jmh
lazy val benchmarking = (project in file("benchmarking"))
  .settings(
    javaOptions in run ++= Seq("-Xms2G", "-Xmx2G"),

    assemblyMergeStrategy in assembly := {
      // discard JDK11 module infos from libs (not required for assembly and JDK8)
      case "module-info.class" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
  )
  .enablePlugins(JmhPlugin)
  .dependsOn(distod)
