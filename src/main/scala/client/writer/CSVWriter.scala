package client.writer

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import CSVWriter._

/**
 *
 * @tparam F the format of the written data
 * @param directory the path of the directory that will contain the resulting csv file.
 * @param name the name of the file without file extension.
 *             If the name is not given it will be created using the current timestamp
 * @param header list of strings used as a header in the file
 * @param sep the used separator
 */
class CSVWriter[F <: FileFormat](directory: Path,
                                 name: Option[String] = None,
                                 header: Option[Seq[String]] = None,
                                 sep: Char = ',') {

  private lazy val defaultName: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern(PathFormat))
  private val file: File = new File(directory + File.separator + name.getOrElse(defaultName) + ".csv")
  private val outputFile: BufferedWriter = new BufferedWriter(new FileWriter(file))
  outputFile.append(header.getOrElse(List()).mkString(",") + {if (header.isDefined) "\n" else ""})

  /**
   * Append a sequence of data rows in the file
   * @param row sequence of rows containing data
   */
  def appendBlock(row: Seq[F]): Unit = {
    outputFile.append(row.map(_.productIterator.map(_.toString).mkString(sep.toString)).mkString("\n") + "\n")
  }

  /**
   * Close the stream
   */
  def close(): Unit = {
    outputFile.close()
  }
}

object CSVWriter {
  val NodesPath: Path = Paths.get("resources/client/nodes/")
  val EdgesPath: Path = Paths.get("resources/client/edges/")

  val VerticesFileHeader = List("address")
  val EdgesFileHeader = List("hash", "in", "out", "value")

  private val PathFormat = "YYYYMMdd_HHmmss"
}