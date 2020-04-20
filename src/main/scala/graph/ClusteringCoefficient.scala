package graph

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Contains method to compute local and global clustering coefficients. It is suited for a directed graph, but global
 * clustering coefficients should work also on undirected graphs
 * General details on [[https://en.wikipedia.org/wiki/Clustering_coefficient]]
 */
object ClusteringCoefficient {

  /**
   * Compute the possible number of pairs using binomial coefficient
   * @param n the number of elements
   * @return the number of pairs
   */
  private def pairs(n: Int): Int = {
    @annotation.tailrec
    def pairsHelper(nh: Int, kh: Int, ac: Int): Int = {
      if (kh > 2) ac
      else pairsHelper(nh + 1, kh + 1, (nh * ac) / kh)
    }
    if (2 == n) 1
    else pairsHelper(n - 1, 1, 1)
  }

  /**
   * Return a set containing the vertices with degree strictly greater than one
   * @tparam V the vertex attribute type
   * @tparam E the edge attribute type
   *
   * @param graph the analyzed graph
   * @return a set containing vertices with degree greater than one
   */
  private def verticesWithDegreeGT1[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Set[VertexId] = {
    graph.ops
      .degrees
      .filter(_._2 > 1)
      .map(_._1)
      .collect
      .toSet
  }

  /**
   * A clustering coefficient relative to each vertex of a *directed* graph
   * More details on [[https://en.wikipedia.org/wiki/Clustering_coefficient#Local_clustering_coefficient]]
   * @tparam V the vertex attribute type
   * @tparam E the edge attribute type
   *
   * @param graph the analyzed graph
   * @return an array associating to each vertex its local cluster coefficient
   */
  def localClusteringCoefficient[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Array[(VertexId, Double)] = {

    /*
    To instantiate a graph in a generic context (instantiating a Graph[V, E] where where V and E are a type parameters),
    Scala needs to have information at runtime about V and E, in the form of an implicit value of type ClassTag[V] and
    ClassTag[E]
    */

    // Exclude edges from and to the same vertex
    val newGraph: Graph[V, E] = graph.subgraph(e => e.dstId != e.srcId).cache()

    // Associate to each vertex its directed neighbors without considering the direction of the edge
    val neighbors: VertexRDD[Set[VertexId]] = newGraph
      .aggregateMessages[Set[VertexId]](
        sendMsg = triplet => {
          triplet.sendToDst(Set(triplet.srcId))
          triplet.sendToSrc(Set(triplet.dstId))
        },
        mergeMsg = _ ++ _
      ).cache()

    // Associate to each vertex the set of directed connected vertices considering the edge direction
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
   * Compute the global clustering coefficient.
   * More details on [[https://en.wikipedia.org/wiki/Triadic_closure#Clustering_coefficient]]
   * @tparam V the vertex attribute type
   * @tparam E the edge attribute type
   *
   * @param graph the analyzed graph
   * @return the global cluster coefficient
   */
  def globalClusteringCoefficient[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Double = {
    // Exclude edges from and to the same vertex
    val newGraph: Graph[V, E] = graph.subgraph(e => e.dstId != e.srcId).cache()

    val considerableVertices: Set[VertexId] = verticesWithDegreeGT1(newGraph)

    /*
      Number of triangles for each vertex and then filter those that have degree less than two.
      Filtering here is not strictly necessary but they will be excluded later
    */
    val closedTriplets: VertexRDD[Int] = newGraph.triangleCount()
      .vertices
      .filter(v => considerableVertices.contains(v._1))

    /*
      Number of possible triplets (close or open) as the number of possible adjacent edges pairs, for each vertex.
      Filtering is necessary, otherwise could be a problem computing the pairs
    */
    val possibleTriplets: RDD[(VertexId, Int)] = newGraph.ops
      .degrees
      .filter(v => considerableVertices.contains(v._1))
      .map(v => (v._1, pairs(v._2)))

    (1.0 / considerableVertices.size.toDouble) *
      closedTriplets.join(possibleTriplets).map(v => v._2._1.toDouble / v._2._2.toDouble).sum()
  }

  /**
   * A measure similar to global clustering. It measures the presence of triadic closure.
   * More details on [[https://en.wikipedia.org/wiki/Triadic_closure#Transitivity]]
   *
   * @tparam V the vertex attribute type
   * @tparam E the edge attribute type
   *
   * @param graph the analyzed graph
   * @return the graph transitivity
   */
  def transitivity[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Double = {

    // Exclude edges from and to the same vertex
    val newGraph: Graph[V, E] = graph.subgraph(e => e.dstId != e.srcId).cache()

    // Vertices with degree greater or equal than two
    val considerableVertices: Set[VertexId] = verticesWithDegreeGT1(newGraph)

    // Number of triangles in the graph
    val triangles: Double = newGraph.triangleCount()
      .vertices
      .filter(v => considerableVertices.contains(v._1))
      .map(_._2)
      .sum / 3.0

    // Number of possible triplets (close or open) in the graph
    val allPossibleTriplets: Double = newGraph.ops
      .degrees
      .filter(v => considerableVertices.contains(v._1))
      .map(v => pairs(v._2))
      .sum

    3.0 * triangles / allPossibleTriplets
  }

  /**
   * An alternative method to global clustering coefficient based on the average of the local clustering coefficient.
   * More details on [[https://en.wikipedia.org/wiki/Clustering_coefficient#Network_average_clustering_coefficient]]
   *
   * @tparam V the vertex attribute type
   * @tparam E the edge attribute type
   *
   * @param graph the analyzed graph
   * @return the graph average cluster coefficient
   */
  def averageClusterCoefficient[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Double = {
    val localClusterCoeff: Array[(VertexId, Double)] = localClusteringCoefficient(graph)
    localClusterCoeff.map(_._2).sum / localClusterCoeff.length.toDouble
  }
}
