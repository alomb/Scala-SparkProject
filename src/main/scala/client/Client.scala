package client

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.{Http, HttpExt}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import scala.concurrent.Future
import domain.Blockchain
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._

sealed trait RequestType
case class BlockchainRequest() extends RequestType

sealed trait Messages
case class Start(operationType: RequestType)
case class Get(url: String) extends Messages
case class Success(results: Seq[String]) extends Messages
case class Error(message: String) extends Messages

class ApiClient extends Actor with ActorLogging {
  import context.dispatcher

  private final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  private val http: HttpExt = Http(context.system)

  override def receive: Receive = {
    case HttpResponse(StatusCodes.OK, _, entity, _) =>
      implicit val petFormat: RootJsonFormat[Blockchain] = jsonFormat14(Blockchain)

      val pet: Future[Blockchain] = Unmarshal(entity).to[Blockchain]
      pet.onComplete(x => {
        if (x.isSuccess) {
          context.parent ! Success(Seq(x.get.height.toString, x.get.name))
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

class ApiMaster extends Actor with ActorLogging {

  val client: ActorRef = context.actorOf(Props[ApiClient], "ApiClient")

  override def receive: Receive = {
    case Start(operationType) =>
      operationType match {
        case BlockchainRequest() =>
          client ! Get("https://api.blockcypher.com/v1/eth/main")
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
    master ! Start(BlockchainRequest())
  }
}
