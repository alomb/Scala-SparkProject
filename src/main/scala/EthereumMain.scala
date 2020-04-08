import java.io.File

import graph.GraphUtils
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

object EthereumMain {
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def main(args: Array[String]) {
    println("Program starts")

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    val edges: GraphUtils = new GraphUtils(spark)
    val graph: Graph[String, Long] = edges.createGraphFromObs("resources/graph/edges",
      getListOfFiles("resources/client/nodes").map(_.getAbsolutePath),
      getListOfFiles("resources/client/edges").map(_.getAbsolutePath))

    graph.vertices.foreach(println(_))
    graph.edges.foreach(println(_))


    println("Program ends")
  }
}