package client.writer

import java.io.{BufferedWriter, File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class CSVWriter[F <: FileFormat](path: String, header: Seq[String], sep: Char = ',') {

  private val defaultName: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
  private val file: File = new File(path + defaultName + ".csv")
  private val outputFile: BufferedWriter = new BufferedWriter(new FileWriter(file))
  outputFile.append(header.mkString(","))

  def appendBlock(row: Seq[F]): Unit = {
    outputFile.append("\n" + row.map(_.extract().mkString(sep.toString)).mkString("\n"))
  }

  def close(): Unit = {
    outputFile.close()
  }
}