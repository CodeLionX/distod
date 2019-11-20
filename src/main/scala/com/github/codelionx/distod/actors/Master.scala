package com.github.codelionx.distod.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.Master.Command


object Master {

  sealed trait Command extends CborSerializable

  def apply(): Behavior[Command] = Behaviors.setup( context => new Master(context).start())
}

class Master(context: ActorContext[Command]) {

  def start(): Behavior[Command] = behavior()

  def behavior(): Behavior[Command] = Behaviors.receiveMessage{
    case _ => ???
  }
}
