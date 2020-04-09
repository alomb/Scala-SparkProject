import java.nio.file.Path

import client.writer.CSVWriter._
import graph.GraphUtils
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

object EthereumMain {
  def getListOfFiles(dir: Path): List[Path] = {
    if (dir.toFile.exists && dir.toFile.isDirectory) {
      dir.toFile.listFiles.filter(_.isFile).toList.map(_.toPath)
    } else {
      List[Path]()
    }
  }

  def main(args: Array[String]) {
    println("Program starts")

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    val edges: GraphUtils = new GraphUtils(spark)
    val graph: Graph[String, Long] = edges.createGraphFromObs(getListOfFiles(NodesPath).map(_.toString),
      getListOfFiles(EdgesPath).map(_.toString))

    graph.vertices.foreach(println(_))
    graph.edges.foreach(println(_))

    println("Program ends")
  }
}