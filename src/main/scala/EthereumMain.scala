import java.nio.file.Path

import client.writer.CSVWriter._
import graph.GraphUtils
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
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

    val graphUtils: GraphUtils = new GraphUtils(spark)
    val graph: Graph[String, Long] = graphUtils.createGraphFromObs(getListOfFiles(NodesPath).map(_.toString),
      getListOfFiles(EdgesPath).map(_.toString)).cache()

    //graph.pageRank(0.0001).vertices.foreach(println(_))

    val connectedComponentAddr: RDD[(VertexId, Iterable[String])] = graph
      .connectedComponents
      .vertices
      .join(graph.vertices)
      .map(_._2)
      .groupByKey()

    graphUtils.saveAsGEXF("resources/graph/graph.gexf", graph)

    println("Program ends")
  }
}