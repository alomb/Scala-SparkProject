import graph._
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession

object EthereumMain {

  def main(args: Array[String]) {
    println("Program starts")

    val conf: Configuration = if(args.length == 2) AWSConfiguration(args(0), args(1)) else LocalConfiguration()

    val spark: SparkSession = conf match {
      case _: AWSConfiguration =>
        SparkSession.builder()
          .appName("Main")
          .getOrCreate()
      case _: LocalConfiguration =>
        SparkSession.builder()
          .appName("Main")
          .master("local[*]")
          .getOrCreate()
    }

    val graphUtils: GraphUtils = new GraphUtils(spark)
    val graph: Graph[String, Long] = graphUtils.createGraphFromObs(conf)

    println(s"Global clustering coefficient: \n${ClusteringCoefficient.globalClusteringCoefficient(graph)}")
    println(s"Transitivity: \n${ClusteringCoefficient.transitivity(graph)}")
    println(s"Average clustering coefficient: \n${ClusteringCoefficient.averageClusterCoefficient(graph)}")
    println("Local clustering coefficient: \n")
    ClusteringCoefficient.localClusteringCoefficient(graph).foreach(println(_))

    val greatestSubgraph: Graph[String, Long] = graphUtils.getSubgraphs(graph, 1)
    val clusteredGraph: Graph[VertexId, Long] = CommunityDetection.labelPropagation(greatestSubgraph, 5)

    println(s"Modularity; ${Modularity.run(clusteredGraph)}")

    spark.stop()

    println("Program ends")
  }
}
