package client.writer

import java.io.{BufferedWriter, File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class CSVWriter(path: String, sep: Char = ',', nameTail: Option[String] = None) {

  private val defaultName: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
  private val file: File = new File(path + defaultName + nameTail.getOrElse("") + ".csv")
  private val outputFile: BufferedWriter = new BufferedWriter(new FileWriter(file))

  def writeBlock(row: Seq[Seq[String]]): Unit = {
    outputFile.write(row.map(_.mkString(sep.toString)).mkString("\n"))
  }

  def writeLine(row: Seq[String]): Unit = {
    outputFile.write(row.mkString(sep.toString))
  }

  def close(): Unit = {
    outputFile.close()
  }
}