package client.actors

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, ResponseEntity, StatusCodes}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.http.scaladsl.{Http, HttpExt}
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import client.actors.Worker.Request
import client.extractors.{Extraction, Extractor}
import domain.Ethereum

import scala.concurrent.Future

/**
 * An actor responsible for getting and parsing HTTP requests.
 *
 * 1) The request is supported by the Request-Level Client-Side of akka-http module. The result of a request is piped to
 * the actor itself and then parsed.
 * 2) The parse (unmarshalling) of the JSON object is provided by the akka-http-spray-json module.
 */
class Worker[T <: Ethereum, U <: Extraction](implicit val ex: Extractor[T, U],
                                             implicit val sum: Unmarshaller[ResponseEntity, T],
                                             implicit val mum: Unmarshaller[ResponseEntity, List[T]]) extends Actor with ActorLogging {

  import context.dispatcher

  private implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  private val http: HttpExt = Http(context.system)

  override def postStop(): Unit = {
    http.shutdownAllConnectionPools()
  }

  override def receive: Receive = {

    case Request(url, single) =>
      // Requesting Future[HttpResponse]
      http.singleRequest(HttpRequest(uri = url)).flatMap {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          // Parsing (Future[List[T]])
          if (single) Unmarshal(entity).to[T](sum, dispatcher, materializer).map(List(_))
          else Unmarshal(entity).to[List[T]](mum, dispatcher, materializer)
        case resp @ HttpResponse(code, _, _, _) =>
          log.info("Request failed, response code: " + code)
          resp.discardEntityBytes()
          Future{
            List[T]()
          }
      }.flatMap(obj => {
        // Extracting fields (Future[List[U]])
        Future {ex.extract(obj)}
      }).pipeTo(sender)

    case msg =>
      log.info("Received an unknown message " + msg)
  }
}

object Worker {
  case class Request(url: String, single: Boolean = false)
}
