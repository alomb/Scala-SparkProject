package client

import akka.actor.{ActorSystem, Props}
import client.MasterGraph.ExtractAddresses

import scala.collection.mutable

object Client {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem = ActorSystem("Test")
    val initialSet: mutable.Set[String] = mutable.HashSet("e20afe0400e92530c4baea9c440ce8dbef9965ff77fd8991b3014989f348f4a7",
      "1c710b7f34136b284fd1398e06c458491f847ac0c489d7ab050193e10cb8d805",
      "ddce4c923f091aefcc81eae9e3c1c92798dad7b11cc384e01d0db392ed98ccdd")

    val master = system.actorOf(Props(new MasterGraph(initialSet, 0)), name = "Master")
    master ! ExtractAddresses(0, 3)
  }
}
