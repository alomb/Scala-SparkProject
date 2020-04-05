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
 * - relayed_by: Option[String],
 */
final case class Transaction(block_height: Long,
                             hash: String,
                             addresses: List[String],
                             total: Long,
                             fees: Long,
                             size: Long,
                             gas_used: Long,
                             gas_price: Long,
                             received: String,
                             ver: Int,
                             double_spend: Boolean,
                             vin_sz: Int,
                             vout_sz: Int,
                             confirmations: Long,
                             inputs: List[TransactionInputs],
                             outputs: List[TransactionOutputs])

object Transaction extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionFormat: RootJsonFormat[Transaction] = rootFormat(lazyFormat(jsonFormat16(Transaction.apply)))
}

