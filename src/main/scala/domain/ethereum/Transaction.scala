package domain.ethereum

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
 * A record used to model the Transaction JSON object
 *
 * JSON Optional fields:
 * - internal_txids: Option[List[String]]
 * - confirmed: Option[String]
 * - gas_limit: Option[Long]
 * - contract_creation: Option[Boolean]
 * - receive_count: Option[Long]
 * - block_hash: Option[String]
 * - block_index: Option[Long]
 * - double_of: Option[String]
 * - execution_error: Option[String]
 * - parent_tx: Option[String]
 * - confidence: Option[Long]
 */
final case class Transaction(block_height: Option[Long],
                             hash: Option[String],
                             addresses: Option[List[String]],
                             total: Option[Long],
                             fees: Option[Long],
                             size: Option[Long],
                             gas_used: Option[Long],
                             gas_price: Option[Long],
                             relayed_by: Option[String],
                             received: Option[String],
                             ver: Option[Int],
                             double_spend: Option[Boolean],
                             vin_sz: Option[Int],
                             vout_sz: Option[Int],
                             confirmations: Option[Long],
                             inputs: Option[List[TransactionInputs]],
                             outputs: Option[List[TransactionOutputs]])

object Transaction extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionFormat: RootJsonFormat[Transaction] = rootFormat(lazyFormat(jsonFormat17(Transaction.apply)))
}

