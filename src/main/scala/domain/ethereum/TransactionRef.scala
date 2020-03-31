package domain.ethereum

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

/**
 * A record used to model the TransactionRef JSON object
 *
 * JSON optional fields:
 * - ref_balance: Option[Long]
 * - confirmed: Option[String]
 * - double_of: Option[String]
 */
final case class TransactionRef(block_height: Option[Long],
                                tx_hash: Option[String],
                                tx_input_n: Option[Int],
                                tx_output_n: Option[Int],
                                value: Option[Long],
                                double_spend: Option[Boolean],
                                confirmations: Option[Long],
                                ref_balance: Option[Long],
                                confirmed: Option[String],
                                double_of: Option[String])

object TransactionRef extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionRefFormat: RootJsonFormat[TransactionRef] = jsonFormat10(TransactionRef.apply)
}
