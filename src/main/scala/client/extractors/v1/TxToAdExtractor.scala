package client.extractors.v1

import client.Transaction
import client.extractors.{Extractor, TxToAdExtraction}

/**
 * Extractor used to obtain the data related to a transaction
 */
class TxToAdExtractor extends Extractor[Transaction, TxToAdExtraction] {
  override def extract(objects: List[Transaction]): List[TxToAdExtraction] = {
    objects.map(obj => {
      TxToAdExtraction(obj.hash, obj.inputs, obj.outputs, obj.total)
    })
  }
}