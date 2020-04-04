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
final case class Address(address: Option[String],
                         total_received: Option[Long],
                         total_sent: Option[Long],
                         balance: Option[Long],
                         unconfirmed_balance: Option[Long],
                         final_balance: Option[Long],
                         n_tx: Option[Long],
                         unconfirmed_n_tx: Option[Long],
                         final_n_tx: Option[Long],
                         tx_url: Option[String],
                         txrefs: Option[List[TransactionRef]],
                         unconfirmed_txrefs: Option[List[TransactionRef]],
                         hasMore: Option[Boolean])

object Address extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val addressFormat: RootJsonFormat[Address] = jsonFormat13(Address.apply)
}

