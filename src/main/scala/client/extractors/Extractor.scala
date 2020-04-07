package client.extractors

import domain.Ethereum

trait Extractor[D <: Ethereum, E <: Extraction] {
  def extract(objects: List[D]): List[E]
}