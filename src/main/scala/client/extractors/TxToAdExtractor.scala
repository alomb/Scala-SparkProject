package client.extractors

import domain.Transaction

class TxToAdExtractor extends Extractor[Transaction, TxToAdExtraction] {
  override def extract(objects: List[Transaction]): List[TxToAdExtraction] = {
    objects.map(obj => {
      TxToAdExtraction(obj.hash, obj.inputs, obj.outputs, obj.total)
    })
  }
}