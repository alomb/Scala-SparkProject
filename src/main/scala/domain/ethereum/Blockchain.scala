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
                            low_gas_price: Long)

object Blockchain extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val blockchainFormat: RootJsonFormat[Blockchain] = jsonFormat12(Blockchain.apply)
}
