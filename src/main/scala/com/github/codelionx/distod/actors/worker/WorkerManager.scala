package com.github.codelionx.distod.actors.worker

import akka.actor.typed.{ActorRef, Behavior, Terminated}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.protocols.ResultCollectionProtocol.ResultProxyCommand
import com.github.codelionx.distod.Settings
import com.github.codelionx.distod.actors.master.Master
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand


object WorkerManager {

  val name = "workerManager"

  def apply(
      partitionManager: ActorRef[PartitionCommand], rsProxy: ActorRef[ResultProxyCommand]
  ): Behavior[Receptionist.Listing] = Behaviors.setup { context =>
    context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, context.self)

    val settings = Settings(context.system)
    val numberOfWorkers = settings.numberOfWorkers

    def spawnAndWatchWorker(master: ActorRef[Master.Command], id: Int): Unit = {
      val ref = context.spawn(Worker(partitionManager, rsProxy, master), Worker.name(id), settings.cpuBoundTaskDispatcher)
      context.watch(ref)
    }

    def supervising(masterRef: ActorRef[Master.Command], nextWorkerId: Int): Behavior[Receptionist.Listing] =
      Behaviors
        .receiveMessage[Receptionist.Listing] { case Master.MasterServiceKey.Listing(_) =>
          context.log.error("Master service listing changed despite that the workers are already running!")
          Behaviors.same[Receptionist.Listing]
        }
        .receiveSignal { case (ctx, Terminated(ref)) =>
          ctx.log.error("Worker {} has failed. Starting a new instance", ref)
          spawnAndWatchWorker(masterRef, nextWorkerId)
          supervising(masterRef, nextWorkerId + 1)
        }

    Behaviors.receiveMessage { case Master.MasterServiceKey.Listing(listings) =>
      listings.headOption match {
        case None =>
          Behaviors.same
        case Some(masterRef) =>
          context.log.info("Spawning {} workers", numberOfWorkers)
          for (i <- 0 until numberOfWorkers) {
            spawnAndWatchWorker(masterRef, i)
          }
          supervising(masterRef, numberOfWorkers)
      }
    }
  }
}
