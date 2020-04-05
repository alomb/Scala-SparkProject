package domain.ethereum

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
 * A record used to model the outputs JSON object used inside the Transaction object
 *
 */
final case class TransactionOutputs(value: Long,
                                    script: String,
                                    addresses: List[String])

object TransactionOutputs extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionOutputsFormat: RootJsonFormat[TransactionOutputs] = jsonFormat3(TransactionOutputs.apply)
}