package client.actors.v2

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import akka.pattern.ask
import akka.util.Timeout
import client.actors.Worker
import client.actors.Worker.Request
import client.actors.v2.MasterGraph._
import client.extractors.BkToTxExtraction2
import client.extractors.v2.BkToTxExtractor
import client.writer.CSVWriter.{EdgesPath, NodesPath}
import client.writer.{CSVWriter, EdgeFileFormat, VerticeFileFormat}
import domain.BlockContainer

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}


class MasterGraph(blockNumber: Long, maxBlocks: Int) extends Actor with ActorLogging with Timers {
  private implicit val timeout: Timeout = Timeout(5 seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

  // The nodes of the graph: hash -> (sender node id, receiver node id, total)
  private val edges: mutable.Map[String, (String, String, Long)] = new mutable.HashMap[String, (String, String, Long)]()

  private implicit val bkToTxExtraction: BkToTxExtractor = new BkToTxExtractor()

  // Workers
  private val workerBcToTx: ActorRef = context.actorOf(Props(new Worker[BlockContainer, BkToTxExtraction2]()),
    "workerBkToTx")

  override def receive: Receive = {
    case Start(blocks) =>
      self ! ExtractTransactions(blocks)

    case ExtractTransactions(this.maxBlocks) =>
      self ! End(this.maxBlocks)

    case ExtractTransactions(blocks) =>
      (workerBcToTx ? Request(BkToTxBaseUrl + (blockNumber - blocks).toHexString + "&apikey=" + Token, single = true))
        .mapTo[List[BkToTxExtraction2]]
        .onComplete(block => {
          block match {
            case Success(bc) =>
              edges ++= bc.map(e => (e.hash, (e.from, e.to, e.value))).toMap
            case Failure(bc) =>
              log.info(s"ExtractTransactions: Parsing completed with errors $bc")
          }

          timers.startSingleTimer(TickKey,
            ExtractTransactions(blocks + 1),
            1.second)

          self ! ExtractTransactions(blocks + 1)
        })

    case End(_) =>
      timers.cancel(TickKey)
      val nodes: Set[String] = edges.values.map(_._1).toSet ++ edges.values.map(_._2).toSet
      log.info("Nodes: ")
      nodes.foreach(n => log.info(n toString))
      log.info("Edges: ")
      edges.foreach(e => log.info(e toString))
      context.system.terminate()

      val verticesWriter = new CSVWriter[VerticeFileFormat](NodesPath, header = Some(VerticesFileHeader))
      verticesWriter.appendBlock(nodes.map(VerticeFileFormat).toSeq)
      verticesWriter.close()
      val edgesWriter = new CSVWriter[EdgeFileFormat](EdgesPath, header = Some(EdgesFileHeader))
      edgesWriter.appendBlock(edges.map{case (k, v) => EdgeFileFormat(k, v._1, v._2, v._3)}.toSeq)
      edgesWriter.close()
    case cmd =>
      log.info("Unknown command " + cmd)
  }
}

object MasterGraph {
  // Constant values
  private val BkToTxBaseUrl: String = "https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&boolean=true&tag="

  private val source = scala.io.Source.fromFile("resources/token")
  private val Token = try source.mkString finally source.close()

  private val MaxMultipleRequests: Int = 5

  private val VerticesFileHeader = List("address")
  private val EdgesFileHeader = List("hash", "in", "out", "value")

  // States and messages received by this actor
  sealed trait State
  case class ExtractTransactions(blocks: Int) extends State
  case class End(blocks: Int) extends State
  case class Start(blocks: Int) extends State

  // Object used as a key identifier of the timer
  private case object TickKey
}
