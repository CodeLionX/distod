package com.github.codelionx.distod.actors

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import com.github.codelionx.distod.actors.ShutdownCoordinator.WrappedListing
import com.github.codelionx.distod.protocols.ShutdownProtocol._
import com.github.codelionx.distod.types.ShutdownReason
import com.github.codelionx.distod.Settings

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


object ShutdownCoordinator {

  private final case class WrappedListing(listing: Receptionist.Listing) extends ShutdownCoordinatorCommand

  val name = "shutdown-coordinator"

  def apply(leader: ActorRef[LeaderGuardian.Command]): Behavior[ShutdownCoordinatorCommand] = Behaviors.setup(context =>
    new ShutdownCoordinator(context, leader).start()
  )
}


class ShutdownCoordinator(context: ActorContext[ShutdownCoordinatorCommand], leader: ActorRef[LeaderGuardian.Command]) {

  private val receptionistAdapter: ActorRef[Receptionist.Listing] = context.messageAdapter(WrappedListing)

  def start(): Behavior[ShutdownCoordinatorCommand] = {
    registerShutdownTask()
    context.system.receptionist ! Receptionist.Subscribe(Executioner.ExecutionerServiceKey, receptionistAdapter)

    behavior(Set.empty)
  }

  private def behavior(executioners: Set[ActorRef[ShutdownCommand]]): Behavior[ShutdownCoordinatorCommand] =
    Behaviors.receiveMessage {
      case WrappedListing(Executioner.ExecutionerServiceKey.Listing(listings)) =>
        context.log.debug("Updating node registry to: {}", listings)
        behavior(listings)

      case AlgorithmFinished if executioners.isEmpty =>
        context.log.info("Algorithm finished, shutting down system.")
        context.log.debug("No nodes registered, leader can shut down")

        context.log.info("Stopping leader node!")
        CoordinatedShutdown(context.system).run(ShutdownReason.AlgorithmFinishedReason)
        shuttingDown(executioners, executioners)

      case AlgorithmFinished if executioners.nonEmpty =>
        context.log.info("Algorithm finished, shutting down followers.")
        context.log.debug("Asking nodes {} to shut down.", executioners)
        executioners.foreach(e => e ! PerformShutdown(ShutdownReason.AlgorithmFinished()))
        shuttingDown(executioners, executioners)

      case ForceFollowerShutdown(replyTo) if executioners.isEmpty =>
        context.log.info("No nodes registered")
        replyTo ! ForcedFollowerShutdownComplete
        shuttingDown(executioners, executioners)

      case ForceFollowerShutdown(replyTo) if executioners.nonEmpty =>
        // propagate reason
        val reason = CoordinatedShutdown(context.system).shutdownReason()
        context.log.info("Asking nodes {} to shut down, because {}", executioners, reason)
        executioners.foreach(e => e ! PerformShutdown(ShutdownReason.from(reason)))
        shuttingDown(executioners, executioners, Some(replyTo))
    }

  private def shuttingDown(
      executioners: Set[ActorRef[ShutdownCommand]],
      pendingResonses: Set[ActorRef[ShutdownCommand]],
      replyTo: Option[ActorRef[ForcedFollowerShutdownComplete.type]] = None
  ): Behavior[ShutdownCoordinatorCommand] = Behaviors.receiveMessage {
    case AlgorithmFinished =>
      context.log.warn("We are already in the shutdown process, ignoring request to shut down system")
      Behaviors.same

    case ForceFollowerShutdown(_) =>
      Behaviors.ignore

    case WrappedListing(Executioner.ExecutionerServiceKey.Listing(listings)) =>
      // assume unreachable nodes are already down --> remove them from pending
      val removedEntries = executioners -- listings

      // send shut down to new nodes as well
      val newEntries = listings -- executioners
      newEntries.foreach(_ ! PerformShutdown(ShutdownReason.AlgorithmFinished()))

      context.log.debug("Listing changed, new nodes: {}, removed nodes: {}", newEntries, removedEntries)
      val updatedExecutioners = executioners ++ newEntries
      val updatedPending = pendingResonses -- removedEntries ++ newEntries

      if (updatedPending.isEmpty) {
        context.log.debug("All nodes were shut down, leader can shut down")
        replyTo.foreach(_ ! ForcedFollowerShutdownComplete)

        context.log.info("Stopping leader node!")
        CoordinatedShutdown(context.system).run(ShutdownReason.AlgorithmFinishedReason)
        Behaviors.same
      } else {
        context.log.debug("Missing shut down ACKs: {}", updatedPending.size)
        shuttingDown(updatedExecutioners, updatedPending)
      }
  }

  private def registerShutdownTask(): Unit = {
    val system = context.system
    val ref = context.self

    CoordinatedShutdown(system).addTask("stop-workers", "stop followers") { () =>
      CoordinatedShutdown(system).shutdownReason() match {
        case Some(ShutdownReason.AlgorithmFinishedReason) =>
          Future.successful(Done)
        case _ =>
          import akka.actor.typed.scaladsl.AskPattern._
          import system.executionContext

          implicit val scheduler: Scheduler = system.scheduler
          implicit val timeout: Timeout = Timeout(Settings(system).shutdownTimeout)

          ref.ask[ForcedFollowerShutdownComplete.type](ref =>
            ForceFollowerShutdown(ref)
          ).map(_ => Done)
      }
    }
  }
}
