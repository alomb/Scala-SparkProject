package client.writer

/**
 * It expresses the format of data file row
 */
sealed trait FileFormat extends Product
case class TestFileFormat(field1: String, field2: Int) extends FileFormat
case class VerticeFileFormat(address: String) extends FileFormat
case class EdgeFileFormat(hash: String, in: String, out: String, value: Long) extends FileFormat