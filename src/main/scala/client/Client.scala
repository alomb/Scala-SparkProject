package client

import akka.actor.{ActorRef, ActorSystem, Props}
import client.actors.MasterGraph
import client.actors.MasterGraph.Start

object Client {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem = ActorSystem("Client")

    val master: ActorRef = system.actorOf(Props(new MasterGraph(9737609, 1)),
      name = "Master")

    master ! Start(0, 0)
  }
}
