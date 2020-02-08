package com.github.codelionx.distod.actors

import akka.actor.CoordinatedShutdown
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.ResultCollectorProxy.{RetryTick, Timeout, WrappedListing}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{ResultCommand, _}
import com.github.codelionx.distod.types.OrderDependency

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps


object ResultCollectorProxy {

  private final case class WrappedListing(listing: Receptionist.Listing) extends ResultProxyCommand
  private case object Timeout extends ResultProxyCommand
  private case object RetryTick extends ResultProxyCommand

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
    timers.startTimerWithFixedDelay("retryTick", RetryTick, 2 seconds)

    Behaviors.receiveMessage {
      case m: FoundDependencies =>
        context.log.trace("Stashing message: {}", m)
        stash.stash(m)
        Behaviors.same
      case _: AckBatch | Timeout | RetryTick =>
        // should not occur --> ignore
        Behaviors.same
      case WrappedListing(ResultCollector.CollectorServiceKey.Listing(listings)) =>
        withFirstCollector(listings) { collectorRef =>
          context.log.info("Result collector proxy found collector instance and is ready!")
          stash.unstashAll(
            behavior(collectorRef, 0, Seq.empty, Map.empty)
          )
        }
      case FlushAndStop =>
        // nothing to flush --> always successful
        Behaviors.stopped
    }
  }

  private def behavior(
      collector: ActorRef[ResultCommand],
      nextId: Int,
      buffer: Seq[OrderDependency],
      pending: Map[Int, Seq[OrderDependency]]
  ): Behavior[ResultProxyCommand] = Behaviors.receiveMessage {
    case FoundDependencies(deps) =>
      val newBuffer = buffer :++ deps
      if (newBuffer.size >= batchSize) {
        val (batch, stillBuffered) = newBuffer.splitAt(batchSize)
        collector ! DependencyBatch(nextId, batch, context.self)
        behavior(collector, nextId + 1, stillBuffered, pending + (nextId -> batch))

      } else {
        behavior(collector, nextId, newBuffer, pending)
      }

    case AckBatch(id) =>
      behavior(collector, nextId, buffer, pending.removed(id))

    case RetryTick =>
      sendPendingBatches(collector, pending)
      Behaviors.same

    case WrappedListing(ResultCollector.CollectorServiceKey.Listing(listings)) =>
      context.log.warn(
        "Collector service listing changed during runtime to {}. This could lead to results being lost.",
        listings
      )
      withFirstCollector(listings) { newCollector =>
        context.log.info("Changing result collector reference to {}", newCollector)
        if (pending.nonEmpty) {
          context.log.debug("Sending {} pending batches to new collector", pending.size)
          sendPendingBatches(newCollector, pending)
        }
        behavior(newCollector, nextId, buffer, pending)
      }

    case FlushAndStop if buffer.isEmpty && pending.isEmpty =>
      context.log.info("Flushing buffer in order to shut down.")
      context.log.debug("Buffer and pending buffer are empty, finished.")
      Behaviors.stopped

    case FlushAndStop if buffer.isEmpty && pending.nonEmpty =>
      context.log.info("Flushing buffer in order to shut down.")
      context.log.debug("Nothing to flush, waiting for {} pending acks", pending.size)
      Behaviors.same

    case FlushAndStop if buffer.nonEmpty =>
      context.log.info("Flushing buffer in order to shut down.")
      collector ! DependencyBatch(nextId, buffer, context.self)
      timers.startSingleTimer(timoutTimerKey, Timeout, 2 seconds)
      val newPending = pending + (nextId -> buffer)
      context.log.debug("Flushed buffer, pending acks: {}", newPending.size)
      flushing(collector, newPending)
  }

  private def flushing(
                        collector: ActorRef[ResultCommand],
                        pending: Map[Int, Seq[OrderDependency]]
                      ): Behavior[ResultProxyCommand] = Behaviors.receiveMessage {
    case FoundDependencies(_) =>
      context.log.error("Received additional dependencies in flushing state, they will be lost!")
      Behaviors.same

    case AckBatch(id) =>
      val newPending = pending - id
      if (newPending.isEmpty) {
        context.log.debug("Successfully flushed buffer to result collector")
        timers.cancelAll()
        Behaviors.stopped
      } else {
        context.log.trace("Current pending acks: {}", newPending.keys)
        Behaviors.same
      }

    case RetryTick =>
      sendPendingBatches(collector, pending)
      Behaviors.same

    case Timeout =>
      timers.cancelAll()
      context.log.error("Could not flush dependencies to result collector in time, pending batches will be lost!")
      Behaviors.stopped

    case WrappedListing(ResultCollector.CollectorServiceKey.Listing(_)) =>
      context.log.error("Collector service listing changed in flushing state: ignoring")
      Behaviors.same

    case FlushAndStop =>
      Behaviors.same
  }

  private def withFirstCollector(listings: Set[ActorRef[ResultCommand]])(
      behaviorFactory: ActorRef[ResultCommand] => Behavior[ResultProxyCommand]
  ): Behavior[ResultProxyCommand] =
    listings.headOption.fold(Behaviors.same[ResultProxyCommand])(behaviorFactory)

  private def sendPendingBatches(collector: ActorRef[ResultCommand], pending: Map[Int, Seq[OrderDependency]]): Unit = {
    pending.foreach { case (i, value) =>
      collector ! DependencyBatch(i, value, context.self)
    }
  }
}
