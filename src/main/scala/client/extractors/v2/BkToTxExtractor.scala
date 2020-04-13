package client.extractors.v2

import client.BlockContainer
import client.extractors.{BkToTxExtraction2, Extractor}

/**
 * Extractor used to transform a block to a list of transactions
 */
class BkToTxExtractor extends Extractor[BlockContainer, BkToTxExtraction2] {

  /**
   * Implicitly convert each String to Long when required
   * @param hex the hexadecimal number to convert
   */
  private implicit def hexToLong (hex: String): Long = {
    try {
      java.lang.Long.parseLong(hex.trim, 16)
    } catch {
      case _: NumberFormatException =>
        Long.MaxValue
    }
  }

  /**
   * Remove the first two characters '0x' from the obtained strings
   * @param s the string to process
   */
  private def remove0x(s: String) : String = s.substring(2)

  override def extract(objects: List[BlockContainer]): List[BkToTxExtraction2] = {
    objects
      .flatMap(_.result.transactions)
      .map(obj => BkToTxExtraction2(remove0x(obj.hash),
        remove0x(obj.from),
        remove0x(obj.to),
        remove0x(obj.value)
      ))
  }
}