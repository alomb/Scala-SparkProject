package graph

import java.io.{BufferedWriter, FileWriter}
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
   *
   * @param conf the [[RunConfiguration]] used to load the files
   * @return a [[Graph]] from the data in the files. If the files are empty, or don't exist or have different quantities
   *         an empty graph is returned.
   */
  def createGraphFromObs(conf: RunConfiguration): Graph[String, Long] = {

    val nodes: RDD[(String, VertexId)] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(conf.nodesFolderPath + "*")
      .as[VerticeFileFormat]
      .rdd
      .distinct()
      .map{case VerticeFileFormat(s) => s}
      .zipWithIndex()

    val tmpEdges1: RDD[(String, (String, VertexId))] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(conf.edgesFolderPath + "*")
      .as[EdgeFileFormat]
      .rdd
      .distinct()
      .map{case EdgeFileFormat(_, in, out, v) => (in, (out, v))}

    // RDD[(address2, (VertexId1, value))]
    val tmpEdges2: RDD[(String, (VertexId, Long))] = tmpEdges1.leftOuterJoin(nodes)
      .map(m => (m._2._1._1, (m._2._2.get, m._2._1._2)))

    val edges: RDD[Edge[Long]] = tmpEdges2.leftOuterJoin(nodes)
      .map(m => Edge(m._2._1._1, m._2._2.get, m._2._1._2))

    Graph(nodes.map(_.swap), edges)
  }

  /**
   * Returns a graph containing the greatest subgraphs (connected components)
   * @tparam V the vertex attribute type
   * @tparam E the edge attribute type
   *
   * @param graph the original graph
   * @param quantity the maximum number of subgraphs in the resulting graph
   */
  def getSubgraphs[V: ClassTag, E: ClassTag](graph: Graph[V, E], quantity: Int): Graph[V, E] = {

    // Implicit ordering used to order the pairs (VertexId, Set[VertexId])
    implicit val ordering: Ordering[(VertexId, Set[VertexId])] =
      new Ordering[(VertexId, Set[VertexId])] {
        override def compare(x: (VertexId, Set[VertexId]),
                             y: (VertexId, Set[VertexId])): Int = {
          x._2.size - y._2.size
        }
      }

    // Compute the set of the selected vertices id
    val subgraphVertices: Set[VertexId] = graph.connectedComponents
        .vertices
        .map(v => (v._2, Set(v._1)))
        .reduceByKey(_ ++ _)
        .top(quantity)(ordering)
        .map(v => v._2)
        .reduce(_ ++ _)

    graph.subgraph(t => subgraphVertices.contains(t.srcId) && subgraphVertices.contains(t.dstId),
      (id, _) => subgraphVertices.contains(id))
  }

  /***
   * Save the [[Graph]] as a gexf file to be visualized in Gephi.
   * @tparam V the vertex attribute type
   * @tparam E the edge attribute type
   *
   * @param path the path of the created file.
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
    try {
      val pw: BufferedWriter = new BufferedWriter(new FileWriter(path))
      pw.write(gexf)
      pw.close()
    } catch {
      case e: Exception =>
        println(s"An error occured during the creation of the .gexf file:\n ${e.getMessage}")
    }
  }
}

/**
 * An execution configuration.
 * @param nodesFolderPath the folder in a S3 bucket or local machine containing the nodes of the graph.
 * @param edgesFolderPath the folder in a S3 bucket or local machine containing the edges of the graph
 */
case class RunConfiguration(nodesFolderPath: String, edgesFolderPath: String)