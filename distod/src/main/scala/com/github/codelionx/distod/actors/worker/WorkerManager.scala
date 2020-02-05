package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy, Terminated}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.master.{Master, MasterHelper}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand

import scala.concurrent.duration._
import scala.language.postfixOps


object WorkerManager {

  val name = "workerManager"

  def apply(
      partitionManager: ActorRef[PartitionCommand], rsProxy: ActorRef[ResultProxyCommand]
  ): Behavior[Receptionist.Listing] = Behaviors.setup { context =>
    context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, context.self)

    val settings = Settings(context.system)
    val numberOfWorkers = settings.numberOfWorkers

    def spawnAndWatchWorker(master: ActorRef[MasterHelper.Command], id: Int): Unit = {
      val ref = context.spawn(
        Behaviors
          .supervise(Worker(partitionManager, rsProxy, master))
          .onFailure(
            SupervisorStrategy
              .restart
              .withLoggingEnabled(true)
              .withLimit(3, 5 seconds)
          ),
        Worker.name(id),
        settings.cpuBoundTaskDispatcher
      )
      context.watch(ref)
    }

    def supervising(masterRef: ActorRef[MasterHelper.Command]): Behavior[Receptionist.Listing] =
      Behaviors
        .receiveMessage[Receptionist.Listing] { case Master.MasterServiceKey.Listing(_) =>
          context.log.error("Master service listing changed despite that the workers are already running!")
          Behaviors.same[Receptionist.Listing]
        }
        .receiveSignal { case (ctx, Terminated(ref)) =>
          ctx.log.error("Worker {} has failed despite restart supervision!", ref)
          supervising(masterRef)
        }

    Behaviors.receiveMessage { case Master.MasterServiceKey.Listing(listings) =>
      listings.headOption match {
        case None =>
          Behaviors.same
        case Some(masterRef) =>
          context.log.info("Found master at {}. Spawning {} workers", masterRef, numberOfWorkers)
          for (i <- 0 until numberOfWorkers) {
            spawnAndWatchWorker(masterRef, i)
          }
          supervising(masterRef)
      }
    }
  }
}
