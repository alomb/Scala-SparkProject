package client.extractors

import client.{TransactionInputs, TransactionOutputs, TransactionRef}

/**
 * It collects the names and types of some desired fields.
 * Used for example as a final result of a request-parse-extract process, like the one
 * conducted by the class [[client.actors.Worker]]
 */
sealed trait Extraction

case class BkToTxExtraction(txids: List[String]) extends Extraction

case class TxToAdExtraction(hash: String,
                            in: List[TransactionInputs],
                            out: List[TransactionOutputs],
                            total: Long) extends Extraction

case class AdToTxExtraction(address: String,
                            txrefs: Option[List[TransactionRef]]) extends Extraction


case class BkToTxExtraction2(hash: String, from: String, to: String, value: Long) extends Extraction
