package client.extractors

import domain.Address

class AdToTxExtractor extends Extractor[Address, AdToTxExtraction] {
  override def extract(objects: List[Address]): List[AdToTxExtraction] = {
    objects.map(obj => {
      AdToTxExtraction(obj.address, obj.txrefs, obj.balance)
    })
  }
}