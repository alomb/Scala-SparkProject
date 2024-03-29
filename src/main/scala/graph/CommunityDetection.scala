package graph

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object CommunityDetection {

  /**
   * Run Label Propagation Algorithm (LPA) for detecting communities in networks.
   *
   * @tparam V the vertices attribute type
   * @tparam E the edge attribute type
   *
   * @param initialGraph the graph for which to compute the community affiliation
   * @param maxSteps the number of supersteps of LPA to be performed.
   *
   * @return a graph with vertex attributes containing the label of community affiliation
   */
  def labelPropagation[V: ClassTag, E: ClassTag](initialGraph: Graph[V, E], maxSteps: Int): Graph[VertexId, E] = {

    /**
     * The recursion step of the LPA algorithm where vertices send to neighbors their cluster identifier and nodes
     * evaluate which cluster to choose depending on the maximum count
     */
    @scala.annotation.tailrec
    def iteration(graph: Graph[VertexId, E], step: Int): (Graph[VertexId, E], Int) = {
      val vertices: RDD[(VertexId, VertexId)] = graph.aggregateMessages[Map[VertexId, Long]](
        sendMsg = triplet => {
          triplet.sendToDst(Map[VertexId, Long]((triplet.srcAttr, 1L)))
          triplet.sendToSrc(Map[VertexId, Long]((triplet.dstAttr, 1L)))
        },
        mergeMsg = (m1, m2) => {
          /*
            Merge the two maps summing the values stored in both initial maps. ++ merge duplicate keys by taking the
            value of the right hand operand.
          */
          m1 ++ m2.map {
            case (k, v) => k -> (v + m1.getOrElse(k, 0L))
          }
        }
      ).mapValues(v => v.maxBy(_._2)._1).cache

      // If no vertex has changed we can stop the execution
      if (graph.vertices.join(vertices).filter(v => v._2._1 != v._2._2).count == 0) {
        (graph, step)
      } else {
        val result: Graph[VertexId, E] = Graph(vertices, graph.edges)

        if (step > 0)
          iteration(result, step - 1)
        else
          (result, step)
      }
    }

    // Each node in the network initially creates a community.
    val result: Graph[VertexId, E] = initialGraph
      .mapVertices((id, _) => id)

    val finalResult = iteration(result, maxSteps)

    println(s"Label Propagation terminated in ${maxSteps - finalResult._2} steps")
    finalResult._1
  }
}
