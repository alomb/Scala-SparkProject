package client

import akka.actor.{ActorSystem, Props}
import client.actors.MasterGraph
import client.actors.MasterGraph.Start

object Client {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem = ActorSystem("Test")

    val master = system.actorOf(Props(new MasterGraph(8827609, 20)), name = "Master")
    master ! Start(0, 0)
  }
}
