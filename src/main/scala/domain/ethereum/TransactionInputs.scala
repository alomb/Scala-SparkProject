package domain.ethereum

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
 * A record used to model the "inputs" JSON object used inside the @refTransaction object
 *
 */
final case class TransactionInputs(sequence: Long,
                                   addresses: List[String])

object TransactionInputs extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionInputsFormat: RootJsonFormat[TransactionInputs] = jsonFormat2(TransactionInputs.apply)
}