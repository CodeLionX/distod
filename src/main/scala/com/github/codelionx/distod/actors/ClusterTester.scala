package com.github.codelionx.distod.actors

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.typed.Cluster
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.{ActorSystem, Settings}

import scala.concurrent.duration._
import scala.language.postfixOps


/**
 * Tests serialization, node roles, and cluster communication using the cluster receptionist.
 */
object ClusterTester {

  val PingServiceKey: ServiceKey[PingService.Ping] = ServiceKey("ping")

  val name: String = "cluster-tester"

  def apply(): Behavior[Nothing] = {
    Behaviors.setup[Receptionist.Listing] { context =>
      import context._

      val role = Settings(system).actorSystemRole
      if (role == ActorSystem.LEADER) {
        spawnAnonymous(PingService())
        Behaviors.empty

      } else {
        system.receptionist ! Receptionist.Subscribe(PingServiceKey, self)

        Behaviors.receiveMessagePartial { case PingServiceKey.Listing(listings) =>
          listings.foreach(ps => spawnAnonymous(Pinger(ps)))
          Behaviors.same
        }
      }

    }
      .narrow
  }

  object Pinger {

    sealed trait Command extends CborSerializable
    final case class Pong(from: ActorRef[PingService.Ping]) extends Command
    private final case object Tick extends Command

    def apply(service: ActorRef[PingService.Ping]): Behavior[Command] = Behaviors.withTimers { timers =>
      Behaviors.setup { context =>
        import context._

        timers.startTimerWithFixedDelay("tick", Tick, 2 seconds)

        val member = Cluster(system).selfMember

        Behaviors.receiveMessage {
          case pong: Pong =>
            log.info("Received pong from {}", pong.from)
            Behaviors.same

          case Tick =>
            service ! PingService.Ping(s"I am node ${member.uniqueAddress}, greetings.", self)
            Behaviors.same
        }
      }
    }
  }

  object PingService {

    final case class Ping(message: String, replyTo: ActorRef[Pinger.Pong]) extends CborSerializable

    def apply(): Behavior[Ping] = Behaviors.setup { context =>
      import context._

      system.receptionist ! Receptionist.Register(PingServiceKey, self)

      Behaviors.receiveMessage { message =>
        log.info("Received ping from {}: {}", message.replyTo, message.message)
        message.replyTo ! Pinger.Pong(self)
        Behaviors.same
      }
    }
  }

}
