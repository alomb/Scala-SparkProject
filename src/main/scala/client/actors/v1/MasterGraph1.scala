package client.actors.v1

import akka.actor.{ActorLogging, ActorRef, Props, Timers}
import akka.pattern.ask
import akka.util.Timeout
import client.actors.Worker.Request
import client.actors.v1.MasterGraph1._
import client.actors.{Master, Worker}
import client.extractors.v1.{AdToTxExtractor, BkToTxExtractor, TxToAdExtractor}
import client.extractors.{AdToTxExtraction, BkToTxExtraction, TxToAdExtraction}
import client.writer.{EdgeFileFormat, VerticeFileFormat}
import client.{Address, Block, Transaction}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * The first version of the actor responsible for the graph construction.
 *
 * As the other actors, the logic is based on a finite state machine (FSM) where messages are used to share the state
 * between the states and to communicate to the pool of [[client.actors.Worker]]s in a single-responsibility fashion.
 * This version uses the API provided  by [[https://www.blockcypher.com/dev/ethereum/]] to build a graph interleaving
 * between requests to obtain addresses and requests to get transactions, exploiting the fact that each one fuel the
 * other. This technique may provide more spatial oriented graphs and facilitate long chains of transactions. In
 * general, considering the service limitations it performs very slowly, obtaining a small amount of data before
 * reaching the maximum number of requests.
 */
class MasterGraph1(blockNumber: Long, maxIterations: Int) extends Master with ActorLogging with Timers {
  private implicit val timeout: Timeout = Timeout(5 seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

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

  // The pool of explorable transaction hashes (ordered by insertion)
  private val txsPool: mutable.Set[String] = new mutable.LinkedHashSet[String]()
  // The nodes of the graph: address -> (node id, balance)
  private val nodes: mutable.Set[String] = new mutable.HashSet[String]()
  // The nodes of the graph: hash -> (sender node id, receiver node id, total)
  private val edges: mutable.Map[String, (String, String, Long)] = new mutable.HashMap[String, (String, String, Long)]()

  override def receive: Receive = {
    case Start(iterations, requests) =>
      (workerBcToTx ? Request(BkToTxBaseUrl + blockNumber, single = true))
        .mapTo[List[BkToTxExtraction]]
        .onComplete(block => {
          block match {
            case Success(bc) =>
              txsPool ++= bc.flatMap(_.txids).take(5)
            case Failure(bc) =>
              log.info(s"Start: Parsing completed with errors $bc")
          }
          timers.startSingleTimer(TickKey,
            ExtractAddresses(iterations, requests + 1),
            1.second)
        })
    case ExtractAddresses(this.maxIterations, requests) =>
      self ! End(maxIterations, requests)
    case ExtractAddresses(iterations, MasterGraph1.MaxTotalRequests) =>
      self ! End(iterations, MasterGraph1.MaxTotalRequests)
    case ExtractAddresses(iterations, requests) =>
      /*
        If the pool is empty or the number of avaiable requests is not enough to allow in the worst case other 2
        requests to obtain information about sender and receiver of the last new transaction the master stops.
      */
      if (txsPool.isEmpty || MaxTotalRequests - requests <= 2) {
        self ! End(iterations, requests)
      } else {
        // Retrieve at most MaxMultipleRequests fresh new transactions or a number that considers future requests
        var txToTake = 1
        while((MaxTotalRequests - requests) > txToTake * 3 && txToTake < MaxMultipleRequests)
          txToTake += 1

        val requestedTransactions: mutable.Set[String] = txsPool.take(txToTake)
        // Remove from the pool the parsed transactions
        txsPool --= requestedTransactions

        val requestUrl: String = TxToAdBaseUrl + requestedTransactions.mkString(";")
        val newAds: mutable.Set[String] = new mutable.HashSet[String]()

        (workerTxToAd ? Request(requestUrl, requestedTransactions.size == 1))
          .mapTo[List[TxToAdExtraction]]
          .onComplete {parsedTxs =>
            parsedTxs match {
              case Success(txs) =>
                txs.foreach(el => {
                  // Add new nodes
                  nodes ++= List(el.in.map(_.addresses.head).head, el.out.map(_.addresses.head).head)
                  newAds ++= List(el.in.map(_.addresses.head).head, el.out.map(_.addresses.head).head)
                  // Add edge
                  edges(el.hash) = (el.in.map(_.addresses.head).head,
                    el.out.map(_.addresses.head).head,
                  el.total)
                })
              case Failure(txs) =>
                log.warning(s"ExtractAddresses: Parsing completed with errors $txs")
            }

            timers.startSingleTimer(TickKey,
              ExtractTransactions(iterations, requests + requestedTransactions.size, newAds),
              1.second)
          }
      }
    case ExtractTransactions(this.maxIterations, requests, _) =>
      self ! End(this.maxIterations, requests)
    case ExtractTransactions(iterations, MasterGraph1.MaxTotalRequests, _) =>
      self ! End(iterations, MasterGraph1.MaxTotalRequests)
    case ExtractTransactions(iterations, requests, newAds) =>
      if (newAds.isEmpty) {
        // The addresses are all updated another iteration is started
        self ! ExtractAddresses(iterations + 1, requests)
      } else {
        // Process only one address to minimize possible errors
        val requestUrl: String = AdToTxBaseUrl + newAds.head + "?limit=" + TxRefLimit

        (workerAdToTx ? Request(requestUrl, single = true)).
          mapTo[List[AdToTxExtraction]]
          .onComplete {parsedAds =>
            parsedAds match {
              case Success(ads) =>
                ads.foreach(el => {
                  // Update pool of transactions
                  txsPool ++= el.txrefs.getOrElse(List()).map(_.tx_hash).take(TxRefLimit).toSet
                })
              case Failure(ads) =>
                log.warning(s"ExtractTransactions: Parsing completed with errors $ads")
            }
            timers.startSingleTimer(TickKey,
              ExtractTransactions(iterations, requests + 1, newAds - newAds.head),
              1.second)
          }
      }
    case End(_, _) =>
      timers.cancel(TickKey)
      context.system.terminate()
      log.info(s"Master has finished. Obtained ${nodes.size} nodes and ${edges.size} edges.")

      writeOnCSV(nodes.map(VerticeFileFormat).toSeq,
        edges.map{case (k, v) => EdgeFileFormat(k, v._1, v._2, v._3)}.toSeq)

    case cmd =>
      log.error("Unknown command " + cmd)
  }
}

object MasterGraph1 {
  // Constant values
  private val TxToAdBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/txs/"
  private val AdToTxBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/addrs/"
  private val BkToTxBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/blocks/"
  private val TxRefLimit: Int = 5
  private val MaxMultipleRequests: Int = 3
  private val MaxTotalRequests: Int = 120

  // States and messages received by this actor
  sealed trait StateT
  abstract class State() extends StateT{
    def iterations: Int
    def requests: Int
  }
  case class ExtractAddresses(iterations: Int, requests: Int) extends State
  case class ExtractTransactions(iterations: Int, requests: Int, newAddresses: mutable.Set[String]) extends State
  case class End(iterations: Int, requests: Int) extends State
  case class Start(iterations: Int, requests: Int) extends State

  // Object used as a key identifier of the timer
  private case object TickKey
}