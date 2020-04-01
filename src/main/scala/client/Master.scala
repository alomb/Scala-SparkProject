package client

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import client.Master.{Start, TxsToAdsRequest}
import client.Worker.Get

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class Master extends Actor with ActorLogging {

  val worker: ActorRef = context.actorOf(Props[Worker], "Worker")
  private implicit val timeout: Timeout = Timeout(5 seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case Start(operationType) =>
      operationType match {
        case TxsToAdsRequest(txs) =>
          val r: Future[List[List[String]]] = (worker ? Get("https://api.blockcypher.com/v1/eth/main/txs/" + txs.mkString(";"))).
            mapTo[List[List[String]]]

          r.onComplete(x => {
            if (x.isSuccess) {
              println("Addresses")
              x.foreach(_.foreach(println(_)))
            } else {
              println(x.failed.get)
            }
            context.system.terminate()
          })
        case _ =>
          log.info("Unknown operation")
      }
    case _ =>
      log.info("Unknown message")
  }
}

object Master {
  sealed trait RequestType
  final case class BlockchainRequest() extends RequestType
  final case class TxsToAdsRequest(transactionHash: List[String]) extends RequestType

  case class Start(operationType: RequestType)
  case class UnknownMessage(message: String)
}