package client.writer

import java.io.{BufferedWriter, File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 *
 * @tparam F the format of the written data
 * @param path the path of the written file, the final path will include also timestamp and file extension
 * @param header list of strings used as a header in the file
 * @param sep the used separator
 */
class CSVWriter[F <: FileFormat](path: String, header: Option[Seq[String]] = None, sep: Char = ',') {

  private val defaultName: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
  private val file: File = new File(path + defaultName + ".csv")
  private val outputFile: BufferedWriter = new BufferedWriter(new FileWriter(file))
  outputFile.append(header.getOrElse(List()).mkString(","))

  /**
   * Append a sequence of data rows in the file
   * @param row sequence of rows containing data
   */
  def appendBlock(row: Seq[F]): Unit = {
    outputFile.append("\n" + row.map(_.extract().mkString(sep.toString)).mkString("\n"))
  }

  /**
   * Close the stream
   */
  def close(): Unit = {
    outputFile.close()
  }
}

object CSVWriter {
  val NodesPath: String = "resources/client/nodes/"
  val EdgesPath: String = "resources/client/edges/"
}
