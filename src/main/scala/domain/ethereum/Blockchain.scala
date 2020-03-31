package domain.ethereum

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
 * A record used to model the Blockchain JSON object
 *
 * JSON Optional fields
 * - last_fork_height: Option[Long]
 * - last_fork_hash: Option[String]
 */
final case class Blockchain(name: Option[String],
                            height: Option[Long],
                            hash: Option[String],
                            time: Option[String],
                            latest_url: Option[String],
                            previous_hash: Option[String],
                            previous_url: Option[String],
                            peer_count: Option[Long],
                            unconfirmed_count: Option[Long],
                            high_gas_price: Option[Long],
                            medium_gas_price: Option[Long],
                            low_gas_price: Option[Long])

object Blockchain extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val blockchainFormat: RootJsonFormat[Blockchain] = jsonFormat12(Blockchain.apply)
}
