package domain.ethereum

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

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
                       mrkl_root: String)

object Block extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val blockFormat: RootJsonFormat[Block] = jsonFormat21(Block.apply)
}