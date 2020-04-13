package client.actors.v2

import akka.actor.{ActorLogging, ActorRef, Props, Timers}
import akka.pattern.ask
import akka.util.Timeout
import client.BlockContainer
import client.actors.Worker.Request
import client.actors.v2.MasterGraph2._
import client.actors.{Master, Worker}
import client.extractors.BkToTxExtraction2
import client.extractors.v2.BkToTxExtractor
import client.writer.{EdgeFileFormat, VerticeFileFormat}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * The second version of the actor responsible for the graph construction.
 *
 * As the other actors, the logic is based on a finite state machine (FSM) where messages are used to share the state
 * between the states and to communicate to the pool of [[Worker]]s in a single-responsibility fashion. This version
 * uses the API provided  by [[https://etherscan.io/]] to build a graph interleaving between requests to obtain blocks
 * and requests to get transactions. From each block it is possible to obtain directly the transactions and to save them
 * this allows to obtain a more time-oriented graph moving from transactions of blocks which are placed temporally in
 * the Blockchain. Considering the better service benefits it is possible to obtain more data than the previous version.
 */
class MasterGraph2(blockNumber: Long, maxBlocks: Int) extends Master with ActorLogging with Timers {
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
              log.warning(s"ExtractTransactions: Parsing completed with errors $bc")
          }

          timers.startSingleTimer(TickKey,
            ExtractTransactions(blocks + 1),
            1.second)

          self ! ExtractTransactions(blocks + 1)
        })

    case End(_) =>
      timers.cancel(TickKey)
      context.system.terminate()

      val nodes: Set[String] = edges.values.map(_._1).toSet ++ edges.values.map(_._2).toSet
      log.info(s"Master has finished. Obtained ${nodes.size} nodes and ${edges.size} edges.")

      writeOnCSV(nodes.map(VerticeFileFormat).toSeq,
        edges.map { case (k, v) => EdgeFileFormat(k, v._1, v._2, v._3) }.toSeq)

    case cmd =>
      log.error("Unknown command " + cmd)
  }
}

object MasterGraph2 {
  // Constant values
  private val BkToTxBaseUrl: String =
    "https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&boolean=true&tag="

  private val source = scala.io.Source.fromFile("resources/token")
  private val Token = try source.mkString finally source.close()

  // States and messages received by this actor
  sealed trait State
  case class ExtractTransactions(blocks: Int) extends State
  case class End(blocks: Int) extends State
  case class Start(blocks: Int) extends State

  // Object used as a key identifier of the timer
  private case object TickKey
}
