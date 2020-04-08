package graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class GraphUtils(spark: SparkSession) {
  import spark.implicits._
  private val sparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  def createGraphFromObs(filesNode: Seq[String], filesEdge: Seq[String]): Graph[String, Long] = {
    // TODO Check presence of files

    val mapNodesIndex: collection.Map[String, VertexId] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filesNode:_*)
      .as[String]
      .rdd
      .mapPartitions(x => x.toList.distinct.toIterator)
      .zipWithIndex()
      .collectAsMap()

    val edges: RDD[Edge[Long]] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filesEdge:_*)
      .as[(String, String, String, Long)]
      .rdd
      .mapPartitions(x => x.toList.distinct.toIterator)
      .map{case (_, in, out, v) => Edge(mapNodesIndex(in), mapNodesIndex(out), v)}

    val nodes: RDD[(VertexId, String)] = sparkContext.parallelize(mapNodesIndex.map(v => (v._2, v._1)).toSeq)

    Graph(nodes, edges)
  }

}
