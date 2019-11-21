package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior, Terminated}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand
import com.github.codelionx.distod.Settings


object WorkerManager {

  val name = "workerManager"

  def apply(partitionManager: ActorRef[PartitionCommand]): Behavior[Receptionist.Listing] = Behaviors.setup { context =>
    context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, context.self)

    val settings = Settings(context.system)
    val cores = Runtime.getRuntime.availableProcessors()
    val numberOfWorkers = Math.min(settings.maxWorkers, cores)

    def spawnAndWatchWorker(master: ActorRef[Master.Command], id: Int): Unit = {
      val ref = context.spawn(Worker(partitionManager, master), Worker.name(id))
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
