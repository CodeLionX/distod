package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer, TimerScheduler}
import akka.actor.Cancellable
import akka.actor.typed.receptionist.Receptionist
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.ResultCollectorProxy.WrappedListing
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.{ResultCommand, _}
import com.github.codelionx.distod.types.OrderDependency

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps


object ResultCollectorProxy {

  private final case class WrappedListing(listing: Receptionist.Listing) extends ResultProxyCommand

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

  private val receptionistAdapter = context.messageAdapter(WrappedListing)
  private val batchSize = Settings(context.system).resultBatchSize

  private implicit val executionContext: ExecutionContext =
    context.system.dispatchers.lookup(DispatcherSelector.default())

  def start(): Behavior[ResultProxyCommand] = initialize()

  private def initialize(): Behavior[ResultProxyCommand] = {
    context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, receptionistAdapter)

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
        val cancellable = context.system.scheduler.scheduleWithFixedDelay(0 seconds, 2 seconds) {
          () => collector ! DependencyBatch(nextId, batch, context.self)
        }

        val newPendingAcks = Map(nextId -> cancellable)
        behavior(collector, nextId + 1, stillBuffered, newPendingAcks)

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
  }

  private def withFirstCollector(listings: Set[ActorRef[ResultCommand]])(
      behaviorFactory: ActorRef[ResultCommand] => Behavior[ResultProxyCommand]
  ): Behavior[ResultProxyCommand] =
    listings.headOption.fold(Behaviors.same[ResultProxyCommand])(behaviorFactory)

}
