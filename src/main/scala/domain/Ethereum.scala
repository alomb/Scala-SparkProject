package domain

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

sealed trait Ethereum

/**
 * A record used to model the Block JSON object
 *
 * Optional fields:
 * - tx_url: Option[String]
 * - txrefs: Option[List[TransactionRef]]
 * - unconfirmed_txrefs: Option[List[TransactionRef]]
 * - hasMore: Option[Boolean]
 */
final case class Address(address: String,
                         total_received: Long,
                         total_sent: Long,
                         balance: Long,
                         unconfirmed_balance: Long,
                         final_balance: Long,
                         n_tx: Long,
                         unconfirmed_n_tx: Long,
                         final_n_tx: Long,
                         tx_url: Option[String],
                         txrefs: Option[List[TransactionRef]],
                         unconfirmed_txrefs: Option[List[TransactionRef]],
                         hasMore: Option[Boolean]) extends Ethereum

object Address extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val addressFormat: RootJsonFormat[Address] = jsonFormat13(Address.apply)
}

/**
 * A record used to model the Block JSON object
 *
 * Optional fields:
 * - next_txids: Option[String]
 * - next_internal_txids: Option[String]
 */
final case class Block(internal_txids: List[String],
                       n_tx: Int,
                       txids: List[String],
                       hash: String,
                       height: Long,
                       prev_block: String,
                       prev_block_url: String,
                       uncles: List[String],
                       chain: String,
                       depth: Long,
                       total: Long,
                       fees: Long,
                       size: Long,
                       ver: Int,
                       time: String,
                       received_time: String,
                       coinbase_addr: String,
                       relayed_by: String,
                       nonce: Long,
                       tx_url: String,
                       mrkl_root: String) extends Ethereum

object Block extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val blockFormat: RootJsonFormat[Block] = jsonFormat21(Block.apply)
}

/**
 * A record used to model the Blockchain JSON object
 *
 * JSON Optional fields
 * - last_fork_height: Option[Long]
 * - last_fork_hash: Option[String]
 */
final case class Blockchain(name: String,
                            height: Long,
                            hash: String,
                            time: String,
                            latest_url: String,
                            previous_hash: String,
                            previous_url: String,
                            peer_count: Long,
                            unconfirmed_count: Long,
                            high_gas_price: Long,
                            medium_gas_price: Long,
                            low_gas_price: Long) extends Ethereum

object Blockchain extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val blockchainFormat: RootJsonFormat[Blockchain] = jsonFormat12(Blockchain.apply)
}

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
                             outputs: List[TransactionOutputs]) extends Ethereum

object Transaction extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionFormat: RootJsonFormat[Transaction] = rootFormat(lazyFormat(jsonFormat16(Transaction.apply)))
}

/**
 * A record used to model the "inputs" JSON object used inside the @refTransaction object
 *
 */
final case class TransactionInputs(sequence: Long,
                                   addresses: List[String]) extends Ethereum

object TransactionInputs extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionInputsFormat: RootJsonFormat[TransactionInputs] = jsonFormat2(TransactionInputs.apply)
}

/**
 * A record used to model the outputs JSON object used inside the Transaction object
 *
 */
final case class TransactionOutputs(value: Long,
                                    script: Option[String],
                                    addresses: List[String]) extends Ethereum

object TransactionOutputs extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionOutputsFormat: RootJsonFormat[TransactionOutputs] = jsonFormat3(TransactionOutputs.apply)
}

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
                                double_of: Option[String]) extends Ethereum

object TransactionRef extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val transactionRefFormat: RootJsonFormat[TransactionRef] = jsonFormat9(TransactionRef.apply)
}