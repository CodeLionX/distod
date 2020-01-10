package com.github.codelionx.distod.actors

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.codelionx.distod.actors.ShutdownCoordinator.WrappedListing
import com.github.codelionx.distod.protocols.ShutdownProtocol._


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
    context.system.receptionist ! Receptionist.Subscribe(Executioner.ExecutionerServiceKey, receptionistAdapter)

    behavior(Set.empty)
  }

  def behavior(executioners: Set[ActorRef[ShutdownCommand]]): Behavior[ShutdownCoordinatorCommand] =
    Behaviors.receiveMessage {
      case WrappedListing(Executioner.ExecutionerServiceKey.Listing(listings)) =>
        context.log.debug("Updating node registry to: {}", listings)
        behavior(listings)

      case AlgorithmFinished if executioners.isEmpty =>
        context.log.info("Algorithm finished, shutting down system.")
        context.log.debug("No nodes registered, leader can shut down")
        leader ! LeaderGuardian.Shutdown
        Behaviors.same

      case AlgorithmFinished if executioners.nonEmpty =>
        context.log.info("Algorithm finished, shutting down system.")
        context.log.debug("Asking nodes {} to shut down.", executioners)
        executioners.foreach(_ ! PerformShutdown(context.self))
        shuttingDown(executioners, executioners)

      case ShutdownPerformed(ref) =>
        // ignore
        context.log.warn("Ignoring shutdown ACK from node {}", ref)
        Behaviors.same
    }

  def shuttingDown(executioners: Set[ActorRef[ShutdownCommand]], pendingResonses: Set[ActorRef[ShutdownCommand]]): Behavior[ShutdownCoordinatorCommand] = Behaviors.receiveMessage {
    case AlgorithmFinished =>
      context.log.warn("We are already in the shutdown process, ignoring request to shut down system")
      Behaviors.same

    case ShutdownPerformed(ref) =>
      next(executioners, pendingResonses - ref)

    case WrappedListing(Executioner.ExecutionerServiceKey.Listing(listings)) =>
      // assume unreachable nodes are already down --> remove them from pending
      val removedEntries = executioners -- listings

      // send shut down to new nodes as well
      val newEntries = listings -- executioners
      newEntries.foreach(_ ! PerformShutdown(context.self))

      context.log.debug("Listing changed, new nodes: {}, removed nodes: {}", newEntries, removedEntries)
      val updatedExecutioners = executioners ++ newEntries
      val updatedPending = pendingResonses -- removedEntries ++ newEntries
      next(updatedExecutioners, updatedPending)
  }

  def next(executioners: Set[ActorRef[ShutdownCommand]], pendingResonses: Set[ActorRef[ShutdownCommand]]): Behavior[ShutdownCoordinatorCommand] =
    if (pendingResonses.isEmpty) {
      context.log.debug("All nodes were shut down, leader can shut down")
      leader ! LeaderGuardian.Shutdown
      behavior(executioners)
    } else {
      context.log.debug("Missing shut down ACKs: {}", pendingResonses.size)
      shuttingDown(executioners, pendingResonses)
    }
}
