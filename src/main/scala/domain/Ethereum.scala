package domain

case class Blockchain(name: String,
  height: Long,
  hash: String,
  time: String, //"2016-06-07T23:44:03.834909578Z"
  latest_url: String,
  previous_hash: String,
  previous_url: String,
  peer_count: Long,
  unconfirmed_count: Long,
  high_gas_price: Long,
  medium_gas_price: Long,
  low_gas_price: Long,
  last_fork_height: Long,
  last_fork_hash: String
)
