package client

import akka.actor.{ActorSystem, Props}
import client.actors.MasterGraph
import client.actors.MasterGraph.Start

object Client {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem = ActorSystem("Test")

    val master = system.actorOf(Props(new MasterGraph(9727610, 30)), name = "Master")
    master ! Start(0, 0)
  }
}
