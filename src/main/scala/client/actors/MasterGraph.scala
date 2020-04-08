package client.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import akka.pattern.ask
import akka.util.Timeout
import client.actors.Worker.Request
import client.extractors._
import client.writer.CSVWriter
import domain.{Address, Block, Transaction}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

class MasterGraph(blockNumber: Int, maxIterations: Int) extends Actor with ActorLogging with Timers {
  // To import the timer key
  import MasterGraph._

  private implicit val txToAdExtraction: TxToAdExtractor = new TxToAdExtractor()
  private implicit val adToTxExtraction: AdToTxExtractor = new AdToTxExtractor()
  private implicit val bkToTxExtraction: BkToTxExtractor = new BkToTxExtractor()

  // Workers
  private val workerTxToAd: ActorRef = context.actorOf(Props(new Worker[Transaction, TxToAdExtraction]()),
    "workerTxToAd")
  private val workerAdToTx: ActorRef = context.actorOf(Props(new Worker[Address, AdToTxExtraction]()),
    "workerAdToTx")
  private val workerBcToTx: ActorRef = context.actorOf(Props(new Worker[Block, BkToTxExtraction]()),
    "workerBkToTx")

  private implicit val timeout: Timeout = Timeout(5 seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

  // The pool of explorable transaction hashes
  private val txsPool: mutable.Set[String] = new mutable.HashSet[String]()
  // The nodes of the graph: address -> (node id, balance)
  private val nodes: mutable.Set[String] = new mutable.HashSet[String]()
  // The nodes of the graph: hash -> (sender node id, receiver node id, total)
  private val edges: mutable.Map[String, (String, String)] = new mutable.HashMap[String, (String, String)]()

  private val TxToAdBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/txs/"
  private val AdToTxBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/addrs/"
  private val BkToTxBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/blocks/"
  private val TxRefLimit: Int = 5
  private val MaxMultipleRequests: Int = 3
  private val MaxTotalRequests: Int = 200

  override def receive: Receive = {
    case Start(iterations, requests) =>
      (workerBcToTx ? Request(BkToTxBaseUrl + blockNumber, single = true)).
        mapTo[List[BkToTxExtraction]].
        onComplete(block => {
          block match {
            case Success(bc) =>
              txsPool ++= bc.flatMap(_.txids).take(10)
            case Failure(bc) =>
              log.info(s"Start: Parsing completed with errors $bc")
          }
          timers.startSingleTimer(TickKey,
            ExtractAddresses(iterations, requests + 1),
            1.second)
        })
    case ExtractAddresses(this.maxIterations, requests) =>
      self ! End(maxIterations, requests)
    case ExtractAddresses(iterations, this.MaxTotalRequests) =>
      self ! End(iterations, this.MaxTotalRequests)
    case ExtractAddresses(iterations, requests) =>
      if (txsPool.isEmpty) {
        // Maximum number of requests or iterations has been reached or there are no transactions to analyze
        self ! End(iterations, requests)
      } else {
        // Retrieve at most TxRetrieved fresh new transactions
        val txToTake: Int = Math.min(MaxMultipleRequests, MaxTotalRequests - requests)
        val requestedTransactions: mutable.Set[String] = txsPool.take(txToTake)
        // Remove from the pool the parsed transactions
        txsPool --= requestedTransactions

        val requestUrl: String = TxToAdBaseUrl + requestedTransactions.mkString(";")
        val newAds: mutable.Set[String] = new mutable.HashSet[String]()

        (workerTxToAd ? Request(requestUrl, requestedTransactions.size == 1)).
          mapTo[List[TxToAdExtraction]].
          onComplete {parsedTxs =>
            parsedTxs match {
              case Success(txs) =>
                txs.foreach(el => {
                  // Add new nodes
                  nodes ++= List(el.in.map(_.addresses.head).head, el.out.map(_.addresses.head).head)
                  newAds ++= List(el.in.map(_.addresses.head).head, el.out.map(_.addresses.head).head)
                  // Add edge
                  edges(el.hash) = (el.in.map(_.addresses.head).head,
                    el.out.map(_.addresses.head).head)
                })
              case Failure(txs) =>
                log.info(s"ExtractAddresses: Parsing completed with errors $txs")
            }
            timers.startSingleTimer(TickKey,
              ExtractTransactions(iterations, requests + requestedTransactions.size, newAds),
              1.second)
          }
      }
    case ExtractTransactions(this.maxIterations, requests, _) =>
      self ! End(this.maxIterations, requests)
    case ExtractTransactions(iterations, this.MaxTotalRequests, _) =>
      self ! End(iterations, this.MaxTotalRequests)
    case ExtractTransactions(iterations, requests, newAds) =>
      if (newAds.isEmpty) {
        // The addresses are all updated another iteration is started
        self ! ExtractAddresses(iterations + 1, requests)
      } else {
        // Process only one address to minimize possible errors
        val requestUrl: String = AdToTxBaseUrl + newAds.head + "?limit=" + TxRefLimit

        (workerAdToTx ? Request(requestUrl, single = true)).
          mapTo[List[AdToTxExtraction]].
          onComplete {parsedAds =>
            parsedAds match {
              case Success(ads) =>
                ads.foreach(el => {
                  // Update pool of transactions
                  txsPool ++= el.txrefs.getOrElse(List()).map(_.tx_hash).toSet
                })
              case Failure(ads) =>
                log.info(s"ExtractTransactions: Parsing completed with errors $ads")
            }
            timers.startSingleTimer(TickKey,
              ExtractTransactions(iterations, requests + 1, newAds - newAds.head),
              1.second)
          }
      }
    case End(_, _) =>
      log.info("Nodes: ")
      nodes.foreach(n => log.info(n toString))
      log.info("Edges: ")
      edges.foreach(e => log.info(e toString))
      context.system.terminate()

      val nodesWriter = new CSVWriter("resources/", nameTail = Some("n"))
      nodesWriter.writeBlock(nodes.map(Seq(_)).toSeq)
      nodesWriter.close()
      val edgesWriter = new CSVWriter("resources/", nameTail = Some("e"))
      edgesWriter.writeBlock(edges.map{case (k, v) => Seq(k, v._1, v._2)}.toSeq)
      edgesWriter.close()
    case cmd =>
      log.info("Unknown command " + cmd)
  }
}

object MasterGraph {
  sealed trait StateT
  abstract class State() extends StateT{
    def iterations: Int
    def requests: Int
  }
  case class ExtractAddresses(iterations: Int, requests: Int)
  case class ExtractTransactions(iterations: Int, requests: Int, newAddresses: mutable.Set[String]) extends State
  case class End(iterations: Int, requests: Int) extends State
  case class Start(iterations: Int, requests: Int) extends State

  private case object TickKey
}