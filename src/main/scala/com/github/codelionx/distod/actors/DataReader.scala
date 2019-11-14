package com.github.codelionx.distod.actors

import akka.NotUsed
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.io.CSVParser


object DataReader {

  val name = "data-reader"

  def apply(): Behavior[NotUsed] = Behaviors.setup { context =>
    import context._

    val settings = Settings(system)

    val parser = CSVParser(settings)
    val table = parser.parse()
    println(table)
    Behaviors.stopped
  }
}
