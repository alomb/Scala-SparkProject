package graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Compute the modularity of a clustered graph
 * The modularity is a value between -0.5 ad 1 indicating  to measure the strength of division of a network into modules
 * (also called groups, clusters or communities).  Networks with high modularity have dense connections between the
 * nodes within modules but sparse connections between nodes in different modules.
 *
 * General overview on https://en.wikipedia.org/wiki/Modularity_(networks)
 */
object Modularity {

  /**
   * Compute the modularity of the given graph (considered undirected) using a generalization of the original formula
   * for measuring on a network partitioned into 2 or more communities.
   *
   * Clauset, Aaron and Newman, M. E. J. and Moore, Cristopher (2004). "Finding community structure in very large
   * networks".
   *
   * @tparam  E the edge attribute type
   *
   * @param graph the clustered graph, each node is associated to its cluster id
   * @return the modularity
   */
  def run[E: ClassTag](graph: Graph[VertexId, E]): Double = {
    // Number of edges often referred as m
    val m: Double = graph.numEdges

    // Collapse the graph vertices into clusters
    val clusters: RDD[(VertexId, Set[VertexId])] = graph.vertices
      .map(v => (v._2, Set(v._1)))
      .reduceByKey((v1, v2) => v1 ++ v2)

    /*
      Maps edges between vertices in the same cluster as self edges.
      Maps edges between vertices in different clusters as edges between clusters.
    */
    val edges: RDD[Edge[E]] = graph.triplets
      .map(t => t.copy(t.srcAttr, t.dstAttr))

    // Graph where each node is a cluster
    val graphOfClusters: Graph[Set[VertexId], E] = Graph[Set[VertexId], E](clusters, edges)

    graphOfClusters.aggregateMessages[Map[VertexId, Double]](
      sendMsg = triplet => {
        if(triplet.dstId == triplet.srcId) {
          // Send 0.5 to avoid counting the edges within the same community twice
          triplet.sendToSrc(Map[VertexId, Double]((triplet.dstId, 0.5)))
          triplet.sendToDst(Map[VertexId, Double]((triplet.srcId, 0.5)))
        } else {
          triplet.sendToSrc(Map[VertexId, Double]((triplet.dstId, 1.0)))
          triplet.sendToDst(Map[VertexId, Double]((triplet.srcId, 1.0)))
        }
      },
      mergeMsg = (m1, m2) => {
        /*
          Merge two maps summing the values stored in both initial maps.
          The final value represents for each cluster the number of edges with another.
        */
        m1 ++ m2.map {
          case (k, v) => k -> (v + m1.getOrElse(k, 0.0))
        }
      }
    ).map(c =>
      /*
      * Modularity for each cluster wrt to its connected clusters.
      *
      * The rationale is the fraction of edges that fall within communities, minus the expected value of the same
      * quantity if edges fall at random without regard for the community structure, which is the total degree of the
      * cluster. Greater the value better is the analyzed clustering.
      */
      (c._1, (c._2.filter(_._1 == c._1).values.sum / (2.0 * m)) - math.pow(c._2.values.sum / (2.0 * m), 2))
    ).values
      .sum
  }
}
