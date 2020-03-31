package client

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{Http, HttpExt}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import domain.ethereum.Transaction

import scala.concurrent.Future

sealed trait RequestType
final case class BlockchainRequest() extends RequestType
final case class TransactionRequest(transactionHash: String) extends RequestType

sealed trait Messages
case class Start(operationType: RequestType)
case class Get(url: String) extends Messages
case class Success(results: Seq[String]) extends Messages
case class Error(message: String) extends Messages

class ApiClient extends Actor with ActorLogging {
  import context.dispatcher

  private final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  private val http: HttpExt = Http(context.system)

  override def postStop(): Unit = http.shutdownAllConnectionPools()

  override def receive: Receive = {
    case HttpResponse(StatusCodes.OK, _, entity, _) =>

      val transaction: Future[Transaction] = Unmarshal(entity).to[Transaction]

      transaction.onComplete(x => {
        if (x.isSuccess) {
          context.parent ! Success(x.get.addresses.getOrElse(Seq()))
        } else {
          context.parent ! Error("Json parsing error.")
        }
      })
    case resp @ HttpResponse(code, _, _, _) =>
      resp.discardEntityBytes()
      context.parent ! Error("Request failed, response code: " + code)
    case Get(url) =>
      http.singleRequest(HttpRequest(uri = url)).pipeTo(self)
  }
}

object ApiMaster {

}

class ApiMaster extends Actor with ActorLogging {

  val client: ActorRef = context.actorOf(Props[ApiClient], "ApiClient")

  case class ApiMasterCommand()

  override def receive: Receive = {
    case Start(operationType) =>
      operationType match {
        case TransactionRequest(transactionHash) =>
          client ! Get("https://api.blockcypher.com/v1/eth/main/txs/" + transactionHash)
      }
    case Success(results) =>
      results.foreach(log.info(_))
      context.system.terminate()
    case Error(info) =>
      log.info(info)
      context.system.terminate()
  }
}

object Client {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Test")

    val master = system.actorOf(Props[ApiMaster], name = "Master")
    master ! Start(TransactionRequest("8f39fb4940c084460da00a876a521ef2ba84ad6ea8d2f5628c9f1f8aeb395342"))
  }
}
