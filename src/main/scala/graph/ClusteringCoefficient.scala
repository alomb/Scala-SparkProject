package graph

import org.apache.spark.graphx.{Graph, PartitionStrategy, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.annotation._
import scala.reflect.ClassTag


/**
 * Contains method to compute local and global cluster coefficients
 * General details on https://en.wikipedia.org/wiki/Clustering_coefficient
 */
object ClusteringCoefficient {

  /**
   * A cluster coefficient relative to each vertex of the graph
   * More details on https://en.wikipedia.org/wiki/Clustering_coefficient
   */
  def localClusteringCoefficient[V: ClassTag, E: ClassTag](graph: Graph[V, E],
                                                           directed : Boolean = true): Array[(VertexId, Double)] = {

    // Exclude edges from and to the same node
    val newGraph: Graph[V, E] = Graph(graph.vertices, graph.edges.filter(e => e.dstId != e.srcId)).cache()

    // Associate to each vertex its neighbors
    val neighbors: VertexRDD[Set[VertexId]] = newGraph
      .aggregateMessages[Set[VertexId]](
        sendMsg = triplet => {
          triplet.sendToDst(Set(triplet.srcId))
          triplet.sendToSrc(Set(triplet.dstId))
        },
        mergeMsg = _ ++ _
      ).cache()

    // Associate to each vertex the reacheable nodes
    val outNeighbors: Map[VertexId, Set[VertexId]] = Graph(neighbors, newGraph.edges)
      .aggregateMessages[Set[VertexId]](
        sendMsg = triplet => {
          triplet.sendToSrc(Set(triplet.dstId))
        },
        mergeMsg = _ ++ _
      ).collect.toMap

    // Count the edges between vertices belonging to each vertex neighborhood
    val edgesBetweenNeighbors: RDD[(VertexId, Double)] = neighbors
      .filter(_._2.size >= 2)
      .map(v => {
        (v._1, v._2.toSeq.map(v2 => {
          v._2.intersect(outNeighbors.getOrElse(v2, Set())).size
        }).sum)
    })

    // Compute the coefficient
    neighbors.join(edgesBetweenNeighbors)
      .filter(v => v._2._1.size >= 2)
      .map(v => (v._1, v._2._2.toDouble / (v._2._1.size * (v._2._1.size - 1)).toDouble))
      .collect
  }

  /**
   *
   * More details on https://en.wikipedia.org/wiki/Triadic_closure#Clustering_coefficient
   */
  def globalClusteringCoefficient[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Double = {

    /**
     * Compute the binomial coefficient
     */
    val coeffBin = (n: Int, k: Int) => {
      @tailrec
      def coeffBinHelper(nc: Int, kc: Int, acc: Int): Int = {
        if (kc > k) acc else coeffBinHelper(nc + 1, kc + 1, (nc * acc) / kc)
      }
      (n, k) match {
        case (0, n) => 1
        case _ => coeffBinHelper(n - k + 1, 1, 1)
      }
    }

    // Exclude edges from and to the same node
    val newGraph: Graph[V, E] = Graph(graph.vertices, graph.edges.filter(e => e.dstId != e.srcId)).cache()

    // Vertices with degree greater or equal than two
    val considerableVertices: Set[VertexId] = newGraph.ops
      .degrees
      .filter(_._2 >= 2)
      .map(_._1)
      .collect
      .toSet

    // Number of triangles for each vertex
    val triangles: VertexRDD[Int] = newGraph.partitionBy(PartitionStrategy.RandomVertexCut)
      .triangleCount()
      .vertices
      .filter(v => considerableVertices.contains(v._1))

    // Number of adjacent edges pairs for each vertex
    val adjacentEdges: RDD[(VertexId, Int)] = newGraph.ops
      .degrees
      .filter(v => considerableVertices.contains(v._1))
      .map(v => (v._1, coeffBin(v._2, 2)))

    (1.toDouble / considerableVertices.size.toDouble) *
      triangles.join(adjacentEdges).map(v => v._2._1.toDouble / v._2._2.toDouble).sum()
  }
}
