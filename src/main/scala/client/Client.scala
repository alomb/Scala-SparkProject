package client

import akka.actor.{ActorSystem, Props}
import client.Master.{Start, TxsToAdsRequest}

object Client {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Test")

    val master = system.actorOf(Props[Master], name = "Master")
    master ! Start(TxsToAdsRequest(List("99ec589ab8cb9498093c4d0c534bdcfd8bd04bfead1d869e3a79165a4f98af0d",
      "ba44f568dc77d33893261c041994eeaef17d108a1845ccb9e376afea2acdd0e9",
      "99ec589ab8cb9498093c4d0c534bdcfd8bd04bfead1d869e3a79165a4f98af0d")))
  }
}
