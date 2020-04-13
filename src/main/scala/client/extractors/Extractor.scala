package client.extractors

import client.Domain

/**
 * The common interface of an extractor
 *
 * @tparam D specific domain type of the extracted data, subtype of [[Domain]]
 * @tparam E resulting extraction, subtype of [[Extraction]]
 */
trait Extractor[D <: Domain, E <: Extraction] {
  /**
   * @param objects list containing the objects to be processed
   * @return the list of extractions
   */
  def extract(objects: List[D]): List[E]
}