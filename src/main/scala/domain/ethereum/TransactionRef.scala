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
 * - block_height: Option[Long]
 */
final case class TransactionRef(tx_hash: String,
                                tx_input_n: Int,
                                tx_output_n: Int,
                                value: Long,
                                double_spend: Boolean,
                                confirmations: Long,
                                ref_balance: Option[Long],
                                confirmed: Option[String],
                                double_of: Option[String])

object TransactionRef extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionRefFormat: RootJsonFormat[TransactionRef] = jsonFormat9(TransactionRef.apply)
}
