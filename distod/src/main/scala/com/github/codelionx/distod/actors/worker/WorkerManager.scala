package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.master.{Master, MasterHelper}
import com.github.codelionx.distod.actors.worker.WorkerManager.{Command, DisconnectTimeout, WrappedListing}
import com.github.codelionx.distod.actors.FollowerGuardian
import com.github.codelionx.distod.actors.FollowerGuardian.Shutdown
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand

import scala.concurrent.duration._
import scala.language.postfixOps


object WorkerManager {

  sealed trait Command
  private final case class WrappedListing(listing: Receptionist.Listing) extends Command
  private final case object DisconnectTimeout extends Command

  val name = "workerManager"

  def apply(
      partitionManager: ActorRef[PartitionCommand], rsProxy: ActorRef[ResultProxyCommand]
  ): Behavior[Command] = Behaviors.setup(context =>
    Behaviors.withTimers(timers =>
      new WorkerManager(context, timers, partitionManager, rsProxy).start()
    )
  )
}


class WorkerManager(
    context: ActorContext[Command],
    timers: TimerScheduler[Command],
    partitionManager: ActorRef[PartitionCommand],
    rsProxy: ActorRef[ResultProxyCommand]
) {

  private val settings = Settings(context.system)
  private val numberOfWorkers = settings.numberOfWorkers

  def start(): Behavior[Command] = {
    val receptionistAdapter = context.messageAdapter(WrappedListing)
    context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, receptionistAdapter)

    Behaviors.receiveMessage {
      case WrappedListing(Master.MasterServiceKey.Listing(listings)) =>
        withFirstRef(listings) { masterRef =>
          context.log.info("Found master at {}", masterRef)
          spawnWorkers(masterRef)
          supervising(masterRef)
        }
    }
  }

  def supervising(masterRef: ActorRef[MasterHelper.Command]): Behavior[Command] =
    Behaviors.receiveMessage[Command] {
      case WrappedListing(Master.MasterServiceKey.Listing(ls)) if ls.isEmpty =>
        context.log.warn("Master service is not available anymore! Waiting {} for it to become reachable again", 30 seconds)
        timers.startSingleTimer("node-disconnect-timeout", DisconnectTimeout, 30 seconds)
        Behaviors.same

      case WrappedListing(Master.MasterServiceKey.Listing(ls)) =>
        withFirstRef(ls) { newMasterRef =>
          if (newMasterRef.equals(masterRef)) {
            context.log.info("Master is available again, resuming work ...")
            timers.cancel("node-disconnect-timeout")
            supervising(newMasterRef)
          } else {
            context.log.warn("Master service listing changed from {} to {}! " +
              "DISTOD can not switch between master nodes as it would mean message loss. Shutting down local node!",
              masterRef,
              newMasterRef
            )
            context.system.asInstanceOf[ActorRef[FollowerGuardian.Command]] ! Shutdown
            Behaviors.stopped
          }
        }

      case DisconnectTimeout =>
        context.log.error("Lost connection to leader node (master), shutting down local node!")
        context.system.asInstanceOf[ActorRef[FollowerGuardian.Command]] ! Shutdown
        Behaviors.stopped
    }

  private def spawnWorkers(masterRef: ActorRef[MasterHelper.Command]): Unit = {
    context.log.info("Spawning {} workers", numberOfWorkers)
    for (i <- 0 until numberOfWorkers) {
      val supervisionStrategy = SupervisorStrategy
        .restart
        .withLoggingEnabled(true)
        .withLimit(3, 5 seconds)
      val ref = context.spawn(
        Behaviors.supervise(Worker(partitionManager, rsProxy, masterRef)).onFailure(supervisionStrategy),
        Worker.name(i),
        settings.cpuBoundTaskDispatcher
      )
      context.watch(ref)
    }
  }

  private def withFirstRef(listings: Set[ActorRef[MasterHelper.Command]])(
      behaviorFactory: ActorRef[MasterHelper.Command] => Behavior[Command]
  ): Behavior[Command] =
    listings.headOption.fold(Behaviors.same[Command])(behaviorFactory)
}