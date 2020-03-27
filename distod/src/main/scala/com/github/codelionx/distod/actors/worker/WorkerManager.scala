package com.github.codelionx.distod.actors.worker

import akka.actor.CoordinatedShutdown
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy, Terminated}
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.master.{Master, MasterHelper}
import com.github.codelionx.distod.actors.worker.WorkerManager.{Command, DisconnectTimeout, Stop, WrappedListing}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand
import com.github.codelionx.distod.types.ShutdownReason
import com.github.codelionx.distod.types.ShutdownReason.AlgorithmFinishedReason

import scala.concurrent.duration._
import scala.language.postfixOps


object WorkerManager {

  sealed trait Command
  private final case class WrappedListing(listing: Receptionist.Listing) extends Command
  private final case object DisconnectTimeout extends Command
  private final case class Stop(reason: Option[CoordinatedShutdown.Reason]) extends Command

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
    registerShutdownTask()
    val receptionistAdapter = context.messageAdapter(WrappedListing)
    context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, receptionistAdapter)
    timers.startSingleTimer("node-disconnect-timeout", DisconnectTimeout, 30 seconds)

    Behaviors.receiveMessage {
      case WrappedListing(Master.MasterServiceKey.Listing(listings)) =>
        withFirstRef(listings) { masterRef =>
          context.log.info("Found master at {}", masterRef)
          spawnWorkers(masterRef)
          timers.cancel("node-disconnect-timeout")
          supervising(masterRef)
        }

      case DisconnectTimeout =>
        context.log.error("Could not connect to leader node (master) in time ({} s), shutting down local node!", 30)
        CoordinatedShutdown(context.system).run(ShutdownReason.ConnectionToMasterLostReason)
        Behaviors.stopped

      case Stop(_) =>
        Behaviors.stopped
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
            context.log.error("Master service listing changed from {} to {}! " +
              "DISTOD can not switch between master nodes as it would mean message loss. Shutting down local node!",
              masterRef,
              newMasterRef
            )
            CoordinatedShutdown(context.system).run(ShutdownReason.ConnectionToMasterLostReason)
            Behaviors.stopped
          }
        }

      case DisconnectTimeout =>
        context.log.error("Lost connection to leader node (master), shutting down local node!")
        CoordinatedShutdown(context.system).run(ShutdownReason.ConnectionToMasterLostReason)
        Behaviors.stopped

      case Stop(Some(ShutdownReason.AlgorithmFinishedReason)) =>
        // stop without waiting for workers, because there are no (pending & available) jobs anyway
        Behaviors.stopped

      case Stop(reason) =>
        def stopWorkers(reason: Option[CoordinatedShutdown.Reason]): Behavior[Command] = {
          context.log.error("Shutting down unexpectedly, because {}", reason)
          if(context.children.isEmpty) {
            Behaviors.stopped
          } else {
            context.children.foreach { child =>
              val worker = child.asInstanceOf[ActorRef[Worker.Command]]
              worker ! Worker.Stop
            }
            // we must wait for the workers to finish processing!
            awaitShutdown()
          }
        }

        if(reason.isEmpty) {
          // if no reason is given, check a second time
          CoordinatedShutdown(context.system).shutdownReason() match {
            case Some(AlgorithmFinishedReason) =>
              Behaviors.stopped
            case r =>
              stopWorkers(r)
          }
        } else {
          stopWorkers(reason)
        }
    }

  private def awaitShutdown(): Behavior[Command] = Behaviors
    .receiveMessage[Command] {
      case WrappedListing(Master.MasterServiceKey.Listing(_)) | Stop(_) | DisconnectTimeout =>
        // ignore
        Behaviors.same
    }.receiveSignal {
      case (_, Terminated(_)) if context.children.nonEmpty =>
        // wait
        Behaviors.same
      case (_, Terminated(_)) if context.children.isEmpty =>
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


  private def registerShutdownTask(): Unit = {
    val cs = CoordinatedShutdown(context.system)
    cs.addActorTerminationTask(
      "stop-workers",
      "stop workers",
      context.self.toClassic,
      Some(Stop(cs.shutdownReason()))
    )
  }
}
