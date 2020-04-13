package client.actors

import akka.actor.Actor
import client.writer.CSVWriter._
import client.writer.{CSVWriter, EdgeFileFormat, VerticeFileFormat}

/**
 * Common interface for the masters
 */
trait Master extends Actor {

  /**
   * Write the resulting graph as a csv file
   * @param nodes the sequence of nodes to write
   * @param edges the sequence of edges to write
   */
  def writeOnCSV(nodes: Seq[VerticeFileFormat], edges: Seq[EdgeFileFormat]): Unit = {
    if(nodes.nonEmpty && edges.nonEmpty) {
      val verticesWriter: CSVWriter[VerticeFileFormat] =
        new CSVWriter[VerticeFileFormat](NodesPath, header = Some(VerticesFileHeader))
      verticesWriter.appendBlock(nodes)
      verticesWriter.close()
      val edgesWriter: CSVWriter[EdgeFileFormat] =
        new CSVWriter[EdgeFileFormat](EdgesPath, header = Some(EdgesFileHeader))
      edgesWriter.appendBlock(edges)
      edgesWriter.close()
    }
  }
}
