package client.extractors

import domain.Domain

/**
 * The common interface of an extractor
 *
 * @tparam D specific domain type of the extracted data
 * @tparam E resulting extraction
 */
trait Extractor[D <: Domain, E <: Extraction] {
  /**
   * @param objects list containing the objects to be processed
   * @return the list of extractions
   */
  def extract(objects: List[D]): List[E]
}