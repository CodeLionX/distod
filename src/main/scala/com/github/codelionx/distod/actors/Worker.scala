package com.github.codelionx.distod.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.Master.DispatchWork
import com.github.codelionx.distod.actors.Worker.{CheckCandidateNode, Command}
import com.github.codelionx.distod.protocols.PartitionManagementProtocol.PartitionCommand
import com.github.codelionx.distod.types.CandidateSet

import scala.collection.BitSet


object Worker {

  sealed trait Command extends CborSerializable
  final case class CheckCandidateNode(
      candidateId: CandidateSet,
      spiltCandidates: BitSet,
      swapCandidates: Seq[(Int, Int)]
  ) extends Command

  def name(n: Int): String = s"worker-$n"

  def apply(partitionManager: ActorRef[PartitionCommand]): Behavior[Nothing] =
    Behaviors.setup[Receptionist.Listing] { context =>
      context.system.receptionist ! Receptionist.Subscribe(Master.MasterServiceKey, context.self)

      Behaviors.receiveMessage { case Master.MasterServiceKey.Listing(listings) =>
        listings.headOption match {
          case None =>
            Behaviors.same
          case Some(masterRef) =>
            Behaviors.setup[Command] { workerContext =>
              new Worker(workerContext, masterRef, partitionManager).start()
            }
        }
        Behaviors.same
      }
    }.narrow
}

class Worker(
    context: ActorContext[Command], master: ActorRef[Master.Command], partitionManager: ActorRef[PartitionCommand]
) {

  def start(): Behavior[Command] = {
    master ! DispatchWork(context.self)
    behavior()
  }

  def behavior(): Behavior[Command] = Behaviors.receiveMessage {
    case CheckCandidateNode(candidateId, spiltCandidates, swapCandidates) =>
      context.log.info("Checking candidate node {}", candidateId)

      for(c <- spiltCandidates) {
        val context = candidateId.x - c
      }
      Behaviors.same
  }
}