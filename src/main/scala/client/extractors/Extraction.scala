package client.extractors

import domain.{TransactionInputs, TransactionOutputs, TransactionRef}

sealed trait Extraction
case class BkToTxExtraction(txids: List[String]) extends Extraction

case class TxToAdExtraction(hash: String,
                            in: List[TransactionInputs],
                            out: List[TransactionOutputs],
                            total: Long) extends Extraction

case class AdToTxExtraction(address: String,
                            txrefs: Option[List[TransactionRef]],
                            balance: Long) extends Extraction
