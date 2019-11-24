package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer, TimerScheduler}
import akka.actor.Cancellable
import akka.actor.typed.receptionist.Receptionist
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.ResultCollectorProxy.{Timeout, WrappedListing}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{ResultCommand, _}
import com.github.codelionx.distod.types.OrderDependency

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps


object ResultCollectorProxy {

  private final case class WrappedListing(listing: Receptionist.Listing) extends ResultProxyCommand
  private case object Timeout extends ResultProxyCommand

  val name = "local-result-proxy"

  def apply(): Behavior[ResultProxyCommand] = Behaviors.setup(context =>
    Behaviors.withTimers(timers =>
      Behaviors.withStash(10)(stash =>
        new ResultCollectorProxy(context, timers, stash).start()
      )
    )
  )
}


class ResultCollectorProxy(
    context: ActorContext[ResultProxyCommand],
    timers: TimerScheduler[ResultProxyCommand],
    stash: StashBuffer[ResultProxyCommand]
) {

  private final val timoutTimerKey = "timeout"

  private val receptionistAdapter = context.messageAdapter(WrappedListing)
  private val batchSize = Settings(context.system).resultBatchSize

  private implicit val executionContext: ExecutionContext =
    context.system.dispatchers.lookup(DispatcherSelector.default())

  def start(): Behavior[ResultProxyCommand] = initialize()

  private def initialize(): Behavior[ResultProxyCommand] = {
    context.system.receptionist ! Receptionist.Subscribe(ResultCollector.CollectorServiceKey, receptionistAdapter)

    Behaviors.receiveMessage {
      case m: FoundDependencies =>
        context.log.debug("Stashing message: {}", m)
        stash.stash(m)
        Behaviors.same
      case _: AckBatch =>
        // should not occur --> ignore
        Behaviors.same
      case WrappedListing(ResultCollector.CollectorServiceKey.Listing(listings)) =>
        withFirstCollector(listings) { collectorRef =>
          context.log.info("Result collector proxy found collector instance and is ready!")
          stash.unstashAll(
            behavior(collectorRef, 0, Seq.empty, Map.empty)
          )
        }
      case FlushAndStop(replyTo) =>
        // nothing to flush --> always successful
        replyTo ! FlushFinished
        Behaviors.stopped
    }
  }

  private def behavior(
      collector: ActorRef[ResultCommand],
      nextId: Int,
      buffer: Seq[OrderDependency],
      pendingAcks: Map[Int, Cancellable]
  ): Behavior[ResultProxyCommand] = Behaviors.receiveMessage {
    case FoundDependencies(deps) =>
      val newBuffer = buffer :++ deps
      if (newBuffer.size >= batchSize) {
        val (batch, stillBuffered) = newBuffer.splitAt(batchSize)
        val cancellable = scheduleSendingBatch(collector, nextId, batch)
        behavior(collector, nextId + 1, stillBuffered, pendingAcks + (nextId -> cancellable))

      } else {
        behavior(collector, nextId, newBuffer, pendingAcks)
      }

    case AckBatch(id) =>
      val cancellable = pendingAcks(id)
      cancellable.cancel()
      behavior(collector, nextId, buffer, pendingAcks.removed(id))

    case WrappedListing(ResultCollector.CollectorServiceKey.Listing(listings)) =>
      context.log.warn("Collector service listing changed during runtime to {}", listings)
      withFirstCollector(listings) { newCollector =>
        context.log.info("Changing result collector reference to {}", newCollector)
        if (pendingAcks.nonEmpty) {
          context.log.error(
            "Lost {} batches, because they were not acknowledged by collector ({}) before changing to {}!",
            pendingAcks.size,
            collector,
            newCollector
          )
        }
        behavior(newCollector, nextId, buffer, pendingAcks.empty)
      }

    case FlushAndStop(replyTo) if buffer.isEmpty =>
      context.log.info("Buffer is empty, flushing not required.")
      replyTo ! FlushFinished
      Behaviors.stopped

    case FlushAndStop(replyTo) if buffer.nonEmpty =>
      val cancellable = scheduleSendingBatch(collector, nextId, buffer)
      timers.startSingleTimer(timoutTimerKey, Timeout, 2 seconds)
      val newPendingAcks = pendingAcks + (nextId -> cancellable)
      context.log.info("Request to flush and stop, pending acks: {}", newPendingAcks.size)
      flushing(newPendingAcks, Seq(replyTo))
  }

  private def flushing(
      pendingAcks: Map[Int, Cancellable],
      ackReceivers: Seq[ActorRef[FlushFinished.type]]
  ): Behavior[ResultProxyCommand] = Behaviors.receiveMessage {
    case FoundDependencies(_) =>
      context.log.error("Received additional dependencies in flushing state, they will be lost!")
      Behaviors.same

    case AckBatch(id) =>
      pendingAcks(id).cancel()
      val newPending = pendingAcks - id
      if (newPending.isEmpty) {
        context.log.info("Successfully flushed buffer to result collector")
        ackReceivers.foreach(_ ! FlushFinished)
        Behaviors.stopped
      } else {
        context.log.debug("Current pending acks: {}", newPending.keys)
        flushing(newPending, ackReceivers)
      }

    case Timeout =>
      context.log.error("Could not flush dependencies to result collector in time, pending batches will be lost!")
      ackReceivers.foreach(_ ! FlushFinished)
      Behaviors.stopped

    case WrappedListing(ResultCollector.CollectorServiceKey.Listing(_)) =>
      context.log.error("Collector service listing changed in flushing state")
      Behaviors.same

    case FlushAndStop(replyTo) =>
      flushing(pendingAcks, ackReceivers :+ replyTo)
  }

  private def withFirstCollector(listings: Set[ActorRef[ResultCommand]])(
      behaviorFactory: ActorRef[ResultCommand] => Behavior[ResultProxyCommand]
  ): Behavior[ResultProxyCommand] =
    listings.headOption.fold(Behaviors.same[ResultProxyCommand])(behaviorFactory)

  private def scheduleSendingBatch(
      collector: ActorRef[ResultCommand], id: Int, batch: Seq[OrderDependency]
  ): Cancellable =
    context.system.scheduler.scheduleWithFixedDelay(0 seconds, 2 seconds) {
      () => collector ! DependencyBatch(id, batch, context.self)
    }
}
