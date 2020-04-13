package graph

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class ClusterCoefficientTest extends FlatSpec {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ClusterCoefficientTest")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  "Global cluster coefficient" should "be " in {
    val vertices: RDD[(VertexId, String)] =
      sc.parallelize(Seq(
        (3L, "A"),
        (7L, "B"),
        (5L, "C"),
        (2L, "D")))
    val edges: RDD[Edge[Unit]] =
      sc.parallelize(Seq(
        Edge(3L, 7L),
        Edge(3L, 2L),
        Edge(2L, 7L),
        Edge(7L, 5L),
        Edge(2L, 5L)))
    val graph: Graph[String, Unit] = Graph(vertices, edges)

    assert(math.abs(ClusteringCoefficient.globalClusteringCoefficient(graph) - 0.833333333333325) < 0.01)
  }

  "Local cluster coefficient" should "be " in {
    val vertices: RDD[(VertexId, String)] =
      sc.parallelize(Seq(
        (1L, "W"),
        (2L, "C"),
        (3L, "MA"),
        (6L, "MI"),
        (4L, "K"),
        (5L, "A")))
    val edges: RDD[Edge[Unit]] =
      sc.parallelize(Seq(
        Edge(1L, 2L),
        Edge(1L, 6L),
        Edge(3L, 1L),
        Edge(3L, 6L),
        Edge(6L, 2L),
        Edge(6L, 4L),
        Edge(2L, 4L),
        Edge(5L, 6L)))
    val graph: Graph[String, Unit] = Graph(vertices, edges)

    assert(ClusteringCoefficient.localClusteringCoefficient(graph) sameElements Array[(VertexId, Double)](
      (1L, 0.3333333333333333),
      (2L, 0.3333333333333333),
      (3L, 0.5),
      (4L, 0.5),
      (6L, 0.15))
    )
  }

  "Transitivity" should "be " in {
    val vertices: RDD[(VertexId, String)] =
      sc.parallelize(Seq(
        (1L, "A"),
        (3L, "B"),
        (2L, "C"),
        (6L, "D")))
    val edges: RDD[Edge[Unit]] =
      sc.parallelize(Seq(
        Edge(1L, 3L),
        Edge(3L, 2L),
        Edge(1L, 2L),
        Edge(2L, 6L)))
    val graph: Graph[String, Unit] = Graph(vertices, edges)

    assert(math.abs(ClusteringCoefficient.transitivity(graph) - 0.6) < 0.001)
  }
}
