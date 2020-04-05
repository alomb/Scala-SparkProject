package client

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import client.MasterGraph.{End, ExtractAddresses, ExtractTransactions}
import client.Worker.Request
import domain.ethereum.{Address, Transaction, TransactionInputs, TransactionOutputs, TransactionRef}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

class MasterGraph(txsPool: mutable.Set[String], max_iterations: Int) extends Actor with ActorLogging {
  private val workerTxToAd: ActorRef = context.actorOf(Props(new Worker[Transaction]()), "workerTxToAd")
  private val workerAdToTx: ActorRef = context.actorOf(Props(new Worker[Address]()), "workerAdToTx")

  private implicit val timeout: Timeout = Timeout(5 seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

  // The nodes of the graph: address -> (node id, balance)
  private val nodes: mutable.Map[String, (Long, Option[Long])] = new mutable.HashMap[String, (Long, Option[Long])]()
  // The nodes of the graph: hash -> (sender node id, receiver node id, total)
  private val edges: mutable.Map[String, (Long, Long, Long)] = new mutable.HashMap[String, (Long, Long, Long)]()

  private val txToAdBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/txs/"
  private val txToAdFields =  Seq("hash", "inputs", "outputs", "total")
  private val adToTxBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/addrs/"
  private val adToTxfields =  Seq("address", "txrefs", "balance")

  private val txRefLimit: Int = 5

  override def receive: Receive = {
    case ExtractAddresses(iteration) =>
      if (iteration == max_iterations) {
        // Maximum number of iterations reached
        self ! End()
      } else {
        val requestedTransactions: mutable.Set[String] = txsPool.take(3)
        val requestUrl: String = txToAdBaseUrl + requestedTransactions.mkString(";")
        txsPool --= requestedTransactions
        println(s"Request Transactions $requestUrl")
        val request: Future[List[List[(Any, String)]]] = (workerTxToAd ? Request(requestUrl, txToAdFields, requestedTransactions.size == 1)).
          mapTo[List[List[(Any, String)]]]
        val newAds: mutable.Set[String] = new mutable.HashSet[String]()

        request onComplete {parsedTxs =>
          parsedTxs match {
            case Success(txs) =>
              txs.foreach(e => {
                // Extract fields
                val hash: String = e.head._1.asInstanceOf[Option[String]].get
                val in: String = e(1)._1.asInstanceOf[Option[List[TransactionInputs]]].get.head.addresses.get.head
                val out: String = e(2)._1.asInstanceOf[Option[List[TransactionOutputs]]].get.head.addresses.get.head
                val tot: Long = e(3)._1.asInstanceOf[Option[Long]].get
                println(s"$in -> $hash ($tot) -> $out")
                // Add new nodes
                List(in, out).
                  filter(!nodes.contains(_)).
                  foreach(a => {
                    newAds += a
                    nodes(a) = (nodes.keySet.size + 1, None)
                  })
                // Add edge
                edges(hash) = (nodes(in)_1, nodes(out)_1, tot)
              })
            case Failure(txs) =>
              println(s"Completed with errors $txs")
          }
          Thread.sleep(1000)
          self ! ExtractTransactions(iteration, requestedTransactions.size, newAds)
          println("_________________________")
        }
      }
    case ExtractTransactions(iteration, requests, newAds) =>
      if (requests == 0) {
        // No requests previously made: process is finished
        self ! End()
      } else if (iteration == max_iterations) {
        // Maximum number of iterations reached: process is finished
        self ! End()
      } else if (newAds.isEmpty) {
        // Addresses all updated get new transactions
        println("_________________________")
        self ! ExtractAddresses(iteration + 1)
      } else {
        // Process only one address to minimize possible errors
        val requestUrl: String = adToTxBaseUrl + newAds.head + "?limit=" + txRefLimit
        println(s"Request Addresses $requestUrl")
        val request: Future[List[List[(Any, String)]]] = (workerAdToTx ? Request(requestUrl, adToTxfields, single = true)).
          mapTo[List[List[(Any, String)]]]
        request onComplete (parsedAds => {
          parsedAds match {
            case Success(ads) =>
              ads.foreach(e => {
                // Extract fields
                val address: String = e.head._1.asInstanceOf[Option[String]].get
                val txrefs: List[String] = e(1)._1.asInstanceOf[Option[List[TransactionRef]]].get.map(_.tx_hash.get)
                val balance: Long = e(2)._1.asInstanceOf[Option[Long]].get
                println(s"$address: $txrefs, $balance")
                // Update pool of transactions
                txsPool ++= txrefs.toSet
                // Update new nodes
                nodes(address) = (nodes(address)._1, Some(balance))
              })
            case Failure(ads) =>
              println(s"Completed with errors $ads")
          }
          Thread.sleep(1000)
          self ! ExtractTransactions(iteration, 1, newAds - newAds.head)
        })
      }
    case End() =>
      println("Nodes: ")
      nodes.foreach(println(_))
      println("Edges: ")
      edges.foreach(println(_))
      context.system.terminate()
    case _ =>
      println("Unknown command")
  }
}

object MasterGraph {
  sealed trait State
  case class ExtractAddresses(iteration: Int) extends State
  case class ExtractTransactions(iteration: Int, requests: Int, newAddresses: mutable.Set[String]) extends State
  case class End() extends State

}