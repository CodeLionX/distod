package com.github.codelionx.distod.actors

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.actors.Executioner.WrappedFlushFinished
import com.github.codelionx.distod.protocols.ResultCollectionProtocol
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{FlushAndStop, FlushFinished}
import com.github.codelionx.distod.protocols.ShutdownProtocol.{PerformShutdown, ShutdownCommand, ShutdownCoordinatorCommand, ShutdownPerformed}


object Executioner {

  val ExecutionerServiceKey: ServiceKey[ShutdownCommand] = ServiceKey("executioner")

  case object WrappedFlushFinished extends ShutdownCommand

  val name = "executioner"

  def apply[T](
                guardian: ActorRef[T],
                rsProxy: ActorRef[ResultCollectionProtocol.ResultProxyCommand],
                shutdownMsg: T
              ): Behavior[ShutdownCommand] = Behaviors.setup(context =>
    new Executioner[T](context, guardian, rsProxy, shutdownMsg).start()
  )
}


class Executioner[T](
                      context: ActorContext[ShutdownCommand],
                      guardian: ActorRef[T],
                      rsProxy: ActorRef[ResultCollectionProtocol.ResultProxyCommand],
                      shutdownMsg: T
                    ) {

  private val rsAdapter = context.messageAdapter[FlushFinished.type](_ => WrappedFlushFinished)

  def start(): Behavior[ShutdownCommand] = {
    // register at the registry
    context.system.receptionist ! Receptionist.Register(Executioner.ExecutionerServiceKey, context.self)

    waitforShutdown()
  }

  private def waitforShutdown(): Behavior[ShutdownCommand] = Behaviors.receiveMessagePartial {
    case PerformShutdown(replyTo) =>
      context.log.info("Received request to perform shut down! Flushing result collector proxy")
      rsProxy ! FlushAndStop(rsAdapter)
      shuttingDown(replyTo)
  }

  private def shuttingDown(replyTo: ActorRef[ShutdownCoordinatorCommand]): Behavior[ShutdownCommand] = Behaviors.receiveMessage {
    case PerformShutdown(newReplyTo) if newReplyTo == replyTo =>
      Behaviors.same

    case PerformShutdown(newReplyTo) if newReplyTo != replyTo =>
      context.log.warn("Shutdown coordinator changed!")
      shuttingDown(newReplyTo)

    case WrappedFlushFinished =>
      replyTo ! ShutdownPerformed(context.self)
      guardian ! shutdownMsg
      Behaviors.stopped
  }
}
