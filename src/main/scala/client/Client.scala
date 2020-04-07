package client

import akka.actor.{ActorSystem, Props}
import client.MasterGraph.Start

object Client {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem = ActorSystem("Test")

    val master = system.actorOf(Props(new MasterGraph(9827672, 1)), name = "Master")
    master ! Start(0, 0)
  }
}
