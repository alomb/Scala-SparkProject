package client.extractors

import domain.Block

class BkToTxExtractor extends Extractor[Block, BkToTxExtraction] {
  override def extract(objects: List[Block]): List[BkToTxExtraction] = {
    objects.map(obj => {
      BkToTxExtraction(obj.txids)
    })
  }
}