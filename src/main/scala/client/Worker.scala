package client

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{Http, HttpExt}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import client.Master.UnknownMessage
import client.Worker.Get
import domain.ethereum.Transaction

import scala.concurrent.Future

/**
 * An actor responsible for getting and parsing HTTP requests.
 *
 * 1) The request is supported by the Request-Level Client-Side of akka-http module. The result of a request is piped to
 * the actor itself and then parsed.
 * 2) The parse (unmarshalling) of the JSON object is provided by the akka-http-spray-json module.
 */
class Worker extends Actor with ActorLogging {
  import context.dispatcher

  private final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  private val http: HttpExt = Http(context.system)

  override def postStop(): Unit = {
    http.shutdownAllConnectionPools()
  }

  override def receive: Receive = {
    case Get(url) =>
      // Requesting Future[HttpResponse]
      http.singleRequest(HttpRequest(uri = url)).flatMap {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          // Parsing (Future[List[Transaction]])
          Unmarshal(entity).to[List[Transaction]]
      }.flatMap {
        // Extracting (Future[List[List[String]]])
        p => Future {p.flatMap(_.addresses)}
      }.pipeTo(sender)

    case _ =>
      sender ! UnknownMessage("Received an unknown message")
  }
}

object Worker {
  case class Get(url: String)
}
