package graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Compute the modularity of a clustered graph
 * More details on https://en.wikipedia.org/wiki/Modularity_(networks)
 */
object Modularity {

  /**
   * Compute the modularity of the given graph
   * @param graph the clustered graph, each node is associated to its cluster id
   * @return the modularity
   */
  def compute[E: ClassTag](graph: Graph[VertexId, E]): Double = {
    val edgesNumber: Double = graph.numEdges

    // Collapse the graph vertices into clusters
    val clusters: RDD[(VertexId, Set[VertexId])] = graph.vertices
      .map(v => (v._2, Set(v._1)))
      .reduceByKey((v1, v2) => v1 ++ v2)

    // Collapse edges within vertices in the graph as self edges
    val edges: RDD[Edge[E]] = graph.triplets
      .map(t => t.copy(t.srcAttr, t.dstAttr))

    val graphOfClusters: Graph[Set[VertexId], E] = Graph[Set[VertexId], E](clusters, edges)

    graphOfClusters.aggregateMessages[Map[VertexId, Double]](
      sendMsg = triplet => {
        triplet.sendToDst(Map[VertexId, Double]((triplet.srcId, 1.0)))
        triplet.sendToSrc(Map[VertexId, Double]((triplet.dstId, 1.0)))
      },
      mergeMsg = (m1, m2) => {
        // Merge the two maps summing the values stored in both initial maps
        m1 ++ m2.map {
          case (k, v) => k -> (v + m1.getOrElse(k, 0.0))
        }
      }
    ).mapValues(m => {
      // Modularity for each cluster wrt to its connected clusters
      m.map(_._2 / (2.0 * edgesNumber)).sum - math.pow(m.values.sum / (2.0 * edgesNumber), 2)
    }).values
      .sum
  }
}
