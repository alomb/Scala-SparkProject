import graph._
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * The entry of the Scala-Spark Project
 */
object EthereumMain {

  /**
   * Execute some operations on the graph. If the resources folders passed as arguments start with "S3" the SparkSession
   * is configured to run remotely on AWS cloud for example via EMR services.
   *
   * @param args the passed arguments. It is possible to pass as arguments the paths of folders containing respectively
   *             the vertices and the edges of the graph. Optionally, in local mode, is possible to pass the file where
   *             the resulting graph can be saved in gexf format as a third argument.
   *
   *             For example
   *
   *             Locally  ->  /home/yourname/graph/nodes/ /home/yourname/graph/edges/ /home/yourname/graph/mygraph.gexf
   *             Remotely ->  s3://bucketname/nodes/ s3://bucketname/edges/
   */
  def main(args: Array[String]) {
    println("Program started")

    val localMode: Try[Boolean] = Try(
      if (args(0).substring(2) == "s3" && args(1).substring(2) == "s3")
        false
      else if (args(0).substring(2) != "s3" && args(1).substring(2) != "s3")
        true
      else
        throw new IllegalStateException("Resources folder passed as arguments are inconsistent.")
    )

    if(localMode.isSuccess) {
      val spark: SparkSession = if (localMode.get) {
        SparkSession.builder()
          .appName("Scala-SparkProject Local")
          .master("local[*]")
          .getOrCreate()
      } else {
        SparkSession.builder()
          .appName("Scala-SparkProject Remote")
          .getOrCreate()
      }

      val conf: RunConfiguration = RunConfiguration(args(0), args(1))

      val graphUtils: GraphUtils = new GraphUtils(spark)
      val graph: Graph[String, Long] = graphUtils.createGraphFromObs(conf)

      println(s"Global clustering coefficient: \n${ClusteringCoefficient.globalClusteringCoefficient(graph)}")
      println(s"Transitivity: \n${ClusteringCoefficient.transitivity(graph)}")
      println(s"Average clustering coefficient: \n${ClusteringCoefficient.averageClusterCoefficient(graph)}")
      val localClustCoeff: Array[(VertexId, Double)] =
        ClusteringCoefficient.localClusteringCoefficient(graph)
      println(s"Local clustering coefficient (omitted cluster zeros = ${localClustCoeff.count(_._2 == 0)}):")
      localClustCoeff.filter(_._2 != 0.0).foreach(println(_))

      val greatestSubgraph: Graph[String, Long] = graphUtils.getSubgraphs(graph, 1)
      val clusteredGraph: Graph[VertexId, Long] = CommunityDetection.labelPropagation(greatestSubgraph, 5)

      println(s"Modularity: ${Modularity.run(clusteredGraph)}")

      if (localMode.get && Try(args(2)).isSuccess) {
        graphUtils.saveAsGEXF(args(2), clusteredGraph)
      }

      spark.stop()
    }

    println("Program ended" + { if(localMode.isFailure) " with errors:\n" + localMode.failed.get.getMessage })
  }
}
