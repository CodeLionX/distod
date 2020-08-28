import sbt.Keys._
import sbt.{Def, _}


/**
 * Takes the application and reference config files for Akka and puts them into a Scala file.
 */
object GenerateConfFiles {

  private val trippleQuotes = "\"\"\""

  val settings: Seq[Setting[_]] = inConfig(Compile)(Seq(
//    sourceGenerators += generateFromResource(sourceManaged, _ / "com" / "github" / "codelionx" / "distod" / "DistodConf.scala", "distod.conf",
//      s"""|package com.github.codelionx.distod
//          |
//          |object DistodConf {
//          |  val conf: String = $trippleQuotes%s$trippleQuotes
//          |}
//          |""".stripMargin
//    ),
    sourceGenerators += generateFromDependencyResource(
      sourceManaged,
      _ / "com" / "github" / "codelionx" / "distod" / "ReferenceConf.scala",
      "akka-actor",
      _ / "reference.conf",
      s"""|package com.github.codelionx.distod
          |
          |object ReferenceConf {
          |  val conf: String = $trippleQuotes%s$trippleQuotes
          |}
          |""".stripMargin
    ),
    sourceGenerators += generateFromDependencyResource(
      sourceManaged,
      _ / "com" / "github" / "codelionx" / "distod" / "ApplicationConf.scala",
      "distod",
      _ / "application.conf",
      s"""|package com.github.codelionx.distod
          |
          |object ApplicationConf {
          |  val conf: String = $trippleQuotes%s$trippleQuotes
          |}
          |""".stripMargin
    )
  ))

  def generateFromResource(dir: SettingKey[File], locate: File => File, resourceName: String, template: String): Def.Initialize[Task[Seq[sbt.File]]] =
    Def.task[Seq[File]] {
      val file = locate(dir.value)
      unmanagedResources.value.find(_.name.endsWith(resourceName)) match {
        case Some(sourceFile) =>
          val content = template.stripMargin.format(IO.read(sourceFile))
          if (!file.exists || IO.read(file) != content)
            IO.write(file, content)
          Seq(file)
        case None => Seq.empty
      }
    }

  def generateFromDependencyResource(
                                      dir: SettingKey[File],
                                      locate: File => File,
                                      jarName: String,
                                      locateResource: File => File,
                                      template: String): Def.Initialize[Task[Seq[sbt.File]]] =
    Def.task[Seq[File]] {
      val file = locate(dir.value)
      val classPath = (dependencyClasspath in Compile).value
      if (!file.exists)
        readFileContentFromDependencyJar(classPath, jarName, locateResource) match {
          case Some(conf) =>
            val content = template.stripMargin.format(conf)
            if (!file.exists || IO.read(file) != content)
              IO.write(file, content)
            Seq(file)
          case None =>
            Seq.empty
        }
      else
        Seq(file)
    }

  def readFileContentFromDependencyJar(classPath: Classpath, jarName: String, locate: File => File): Option[String] = {
    val optionalEntry = classPath.find(entry => entry.get(artifact.key) match {
      case Some(entryArtifact) if entryArtifact.name.startsWith(jarName) => true
      case _ => false
    })
    optionalEntry.map { entry =>
      val jarFile = entry.data
      println(jarFile)
      if (jarFile.isDirectory)
        IO.read(locate(jarFile))
      else
        IO.withTemporaryDirectory { tmpDir =>
          IO.unzip(jarFile, tmpDir)
          IO.read(locate(tmpDir))
        }
    }
  }

}

