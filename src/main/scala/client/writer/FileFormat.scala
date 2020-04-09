package client.writer

/**
 * It expresses the format of data file row
 */
sealed trait FileFormat {
  /**
   * @return the row as a sequence of strings
   */
  def extract(): Seq[String]
}

case class TestFileFormat(field1: String, field2: Int) extends FileFormat {
  override def extract(): Seq[String] = {
    Seq(field1, field2.toString)
  }
}

case class VerticeFileFormat(address: String) extends FileFormat {
  override def extract(): Seq[String] = {
    Seq(address)
  }
}

case class EdgeFileFormat(hash: String, in: String, out: String, value: Long) extends FileFormat {
  override def extract(): Seq[String] = {
    Seq(hash, in, out, value.toString)
  }
}
