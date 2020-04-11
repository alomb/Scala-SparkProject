package client.extractors.v1

import client.extractors.{AdToTxExtraction, Extractor}
import domain.Address

/**
 * Extractor used to obtain the data related to an address
 */
class AdToTxExtractor extends Extractor[Address, AdToTxExtraction] {
  override def extract(objects: List[Address]): List[AdToTxExtraction] = {
    objects.map(obj => {
      AdToTxExtraction(obj.address, obj.txrefs)
    })
  }
}