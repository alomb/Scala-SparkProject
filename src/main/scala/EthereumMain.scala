import java.nio.file.Path

import client.writer.CSVWriter._
import graph.{ClusteringCoefficient, GraphUtils}
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
      .appName("Main")
      .getOrCreate()

    val graphUtils: GraphUtils = new GraphUtils(spark)
    val graph: Graph[String, Long] = graphUtils.createGraphFromObs(getListOfFiles(NodesPath).map(_.toString),
      getListOfFiles(EdgesPath).map(_.toString)).cache()

    //graph.pageRank(0.0001).vertices.foreach(println(_))

    /*
    val connectedComponentAddr: RDD[(VertexId, Iterable[String])] = graph
      .connectedComponents
      .vertices
      .join(graph.vertices)
      .map(_._2)
      .groupByKey()

    connectedComponentAddr.collect().foreach(println(_))
    */

    graph.triplets.collect.foreach(println(_))


    println(s"Global clustering coefficient: \n${ClusteringCoefficient.globalClusteringCoefficient(graph)}")
    println(s"Transitivity: \n${ClusteringCoefficient.transitivity(graph)}")
    println(s"Average clustering coefficient: \n${ClusteringCoefficient.averageCLusterCoefficient(graph)}")
    println("Local clustering coefficient: \n")
    ClusteringCoefficient.localClusteringCoefficient(graph).foreach(println(_))

    graphUtils.saveAsGEXF("resources/graph/graph.gexf", graph)

    println("Program ends")
  }
}