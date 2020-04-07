package client.extractors

import domain.Address

class AdToTxExtractor extends Extractor[Address, AdToTxExtraction] {
  def extract(objects: List[Address]): List[AdToTxExtraction] = {
    objects.map(obj => {
      AdToTxExtraction(obj.address, obj.txrefs, obj.balance)
    })
  }
}

object AdToTxExtractor {
  implicit val adToTxExtraction: AdToTxExtractor = new AdToTxExtractor()
}