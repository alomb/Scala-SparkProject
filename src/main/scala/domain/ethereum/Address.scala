package domain.ethereum

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

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
                         hasMore: Option[Boolean])

object Address extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val addressFormat: RootJsonFormat[Address] = jsonFormat13(Address.apply)
}

