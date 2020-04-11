package client

import akka.actor.{ActorRef, ActorSystem, Props}
import client.actors.v2.MasterGraph.Start
import client.actors.v2.MasterGraph

object Client {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem = ActorSystem("Client")

    val master: ActorRef = system.actorOf(Props(new MasterGraph(6900000, 2)),
      name = "Master")

    master ! Start(0)
  }
}
