resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"

// plugin to generate jars: https://github.com/sbt/sbt-assembly
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

// plugin for test coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
addSbtPlugin("com.codacy" % "sbt-codacy-coverage" % "3.0.3")
