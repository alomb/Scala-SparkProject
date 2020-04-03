package client

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import client.Master.{Start, TxsToAdsRequest}
import client.Worker.Request
import domain.ethereum.{Transaction, TransactionInputs, TransactionOutputs}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class Master extends Actor with ActorLogging {

  val worker: ActorRef = context.actorOf(Props(new Worker[Transaction]()), "Worker")
  private implicit val timeout: Timeout = Timeout(5 seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case Start(operationType) =>
      operationType match {
        case TxsToAdsRequest(txs) =>
          val fields =  Seq("hash", "inputs", "outputs")
          val url = "https://api.blockcypher.com/v1/eth/main/txs/" + txs.mkString(";")
          val r: Future[List[List[(Any, String)]]] = (worker ? Request(url, fields)).mapTo[List[List[(Any, String)]]]
          r.onComplete(x => {
            if (x.isSuccess) {
              println("Completed with success")
              x.get.foreach(e => {
                val hash = e.head._1.asInstanceOf[Option[String]].get
                val in = e(1)._1.asInstanceOf[Option[List[TransactionInputs]]].get.head.addresses.get.head
                val out = e(2)._1.asInstanceOf[Option[List[TransactionOutputs]]].get.head.addresses.get.head
                println(s"$in -> $hash -> $out")
              })
            } else {
              println("Completed with errors")
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