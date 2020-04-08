package client.writer

sealed trait FileFormat {
  def extract(): Seq[String]
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
