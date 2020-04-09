package client.writer

import java.nio.file.{Path, Paths}

import org.scalatest.FlatSpec

class TestCSVWriter extends FlatSpec {

  val testDirPath: Path = Paths.get("resources/client/test/")
  val testFileName: String = "test.csv"

  it should "write a file in the resource folder" in {
    assert(testDirPath.toFile.isDirectory)
    val writer = new CSVWriter[TestFileFormat](testDirPath)
    writer.appendBlock(Seq(TestFileFormat("string", 1)))
    writer.close()
  }
}
