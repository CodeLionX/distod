import com.typesafe.sbt.GitPlugin.autoImport.git
import sbt.{Def, _}
import sbt.Keys._

object VersionGenerator {

  val settings: Seq[Setting[_]] = inConfig(Compile)(Seq(
    sourceGenerators += generateVersion(sourceManaged, _ / "com" / "github" / "codelionx" / "distod" / "Version.scala",
      """|package com.github.codelionx.distod
         |
         |object Version {
         |  val current: String = "%s"
         |  val gitSHA: String = "%s"
         |}
         |""".stripMargin
    )
  ))

  def generateVersion(dir: SettingKey[File], locate: File => File, template: String): Def.Initialize[Task[Seq[sbt.File]]] =
    Def.task[Seq[File]]{
      val file = locate(dir.value)
      val content = template.stripMargin.format(version.value, git.gitHeadCommit.value.getOrElse(git.gitCurrentBranch.value))
      if(!file.exists || IO.read(file) != content)
        IO.write(file, content)
      Seq(file)
    }
}
