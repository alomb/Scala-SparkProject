package graph

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import client.writer.{EdgeFileFormat, VerticeFileFormat}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/***
 * Collects some utility functions
 * @param spark The spark session used to parallelize in the cluster the below operations
 */
class GraphUtils(spark: SparkSession) {
  import spark.implicits._
  private val sparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  /***
   * Create a [[Graph]] from multiple csv files containing raw observations
   * @param filesVertice collection of csv file paths containing addresses and associated information
   * @param filesEdge collection of csv file paths containing transactions between addresses and associated information
   * @return a [[Graph]] from the data in the files. If the files are empty, or don't exist or have different quantities
   *         an empty graph is returned.
   */
  def createGraphFromObs(filesVertice: Seq[String], filesEdge: Seq[String]): Graph[String, Long] = {
    val existingVerticesPaths: Seq[String] = filesVertice.filter(f => Files.exists(Paths.get(f)))
    val existingEdgesPaths: Seq[String] = filesEdge.filter(f => Files.exists(Paths.get(f)))

    if(existingVerticesPaths.isEmpty ||
      existingEdgesPaths.isEmpty ||
      existingEdgesPaths.size != existingVerticesPaths.size) {

      return Graph(spark.sparkContext.emptyRDD, spark.sparkContext.emptyRDD)
    }

    val mapNodesIndex: collection.Map[String, VertexId] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(existingVerticesPaths:_*)
      .as[VerticeFileFormat]
      .rdd
      .mapPartitions(x => x.toList.distinct.toIterator)
      .map{case VerticeFileFormat(s) => s}
      .zipWithIndex()
      .collectAsMap()

    val edges: RDD[Edge[Long]] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(existingEdgesPaths:_*)
      .as[EdgeFileFormat]
      .rdd
      .mapPartitions(x => x.toList.distinct.toIterator)
      .map{case EdgeFileFormat(_, in, out, v) => Edge(mapNodesIndex(in), mapNodesIndex(out), v)}

    val nodes: RDD[(VertexId, String)] = sparkContext.parallelize(mapNodesIndex.map(v => (v._2, v._1)).toSeq)

    Graph(nodes, edges)
  }

  /**
   * Returns a graph containing the greatest subgraphs (connected components)
   * @param graph the original graph
   * @param quantity the maximum number of subgraphs in the resulting graph
   */
  def getSubgraphs[V: ClassTag, E: ClassTag](graph: Graph[V, E], quantity: Int): Graph[V, E] = {
    implicit val ordering: Ordering[(VertexId, Iterable[(VertexId, VertexId)])] =
      new Ordering[(VertexId, Iterable[(VertexId, VertexId)])] {
        override def compare(x: (VertexId, Iterable[(VertexId, VertexId)]),
                             y: (VertexId, Iterable[(VertexId, VertexId)])): Int = {
        y._2.size - x._2.size
      }
    }

    val subgraphVertices: Set[VertexId] = graph.connectedComponents()
      .vertices
      .groupBy(_._2)
      .takeOrdered(quantity)(ordering)
      .map(_._2.toSet
        .map((t: (VertexId, VertexId)) => t._1))
      .reduce(_ ++ _)

    graph.subgraph(t => subgraphVertices.contains(t.srcId) && subgraphVertices.contains(t.dstId),
      (id, _) => subgraphVertices.contains(id))
  }

  /***
   * Save the [[Graph]] as a gexf file to be visualized in Gephi
   * @param path the path of the created file
   * @param graph the graph to save.
   */
  def saveAsGEXF[V: ClassTag, E: ClassTag](path: String, graph: Graph[V, E]): Unit = {
    val date: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
    val description: String = "A graph containing some Ethereum transactions"
    val separator: String = "\n\t\t"
    val vertices: String = graph.vertices
      .map(v => s"""<node id=\"${v._1}\" label=\"${v._2}\" />""")
      .collect()
      .mkString(separator)
    val edges: String = graph.edges
      .map(e => s"""<edge source=\"${e.srcId}\" target=\"${e.dstId}\" label=\"${e.attr}\" />""")
      .collect()
      .mkString(separator)

    val gexf: String = s"""<?xml version="1.0" encoding="UTF-8"?>
                          |<gexf xmlns="http://www.gexf.net/1.2draft" version="1.3">
                          |  <meta lastmodifieddate="$date">
                          |    <creator>Alessandro Lombardi</creator>
                          |    <description>$description</description>
                          |  </meta>
                          |  <graph mode="static" defaultedgetype="directed">
                          |    <nodes>
                          |      $vertices
                          |    </nodes>
                          |    <edges>
                          |      $edges
                          |    </edges>
                          |  </graph>
                          |</gexf>""".stripMargin

    val pw: BufferedWriter = new BufferedWriter(new FileWriter(path))
    pw.write(gexf)
    pw.close()
  }
}
