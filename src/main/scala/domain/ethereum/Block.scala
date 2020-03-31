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
final case class Block(internal_txids: Option[List[String]],
                       n_tx: Option[Int],
                       txids: Option[List[String]],
                       hash: Option[String],
                       height: Option[Long],
                       prev_block: Option[String],
                       prev_block_url: Option[String],
                       uncles: Option[List[String]],
                       chain: Option[String],
                       depth: Option[Long],
                       total: Option[Long],
                       fees: Option[Long],
                       size: Option[Long],
                       ver: Option[Int],
                       time: Option[String],
                       received_time: Option[String],
                       coinbase_addr: Option[String],
                       relayed_by: Option[String],
                       nonce: Option[Long],
                       tx_url: Option[String],
                       mrkl_root: Option[String])

object Block extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val blockFormat: RootJsonFormat[Block] = jsonFormat21(Block.apply)
}