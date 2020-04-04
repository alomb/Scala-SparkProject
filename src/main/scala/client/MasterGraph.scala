package client

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import akka.pattern.ask
import client.MasterGraph.{End, ExtractAddresses, ExtractTransactions}
import client.Worker.Request
import domain.ethereum.{Address, Transaction, TransactionInputs, TransactionOutputs, TransactionRef}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class MasterGraph(txsPool: mutable.Set[String], max_iterations: Int) extends Actor with ActorLogging {
  private val workerTxToAd: ActorRef = context.actorOf(Props(new Worker[Transaction]()), "workerTxToAd")
  private val workerAdToTx: ActorRef = context.actorOf(Props(new Worker[Address]()), "workerAdToTx")

  private implicit val timeout: Timeout = Timeout(5 seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

  // The nodes of the graph: address -> (node id, balance)
  private val nodes: mutable.Map[String, (Long, Option[Long])] = new mutable.HashMap[String, (Long, Option[Long])]()
  // The nodes of the graph: hash -> (sender node id, receiver node id, total)
  private val edges: mutable.Map[String, (Long, Long, Long)] = new mutable.HashMap[String, (Long, Long, Long)]()

  private val txsBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/txs/"
  private val addrBaseUrl: String = "https://api.blockcypher.com/v1/eth/main/addrs/"
  private val txRefLimit: Int = 5

  override def receive: Receive = {
    case ExtractAddresses(iteration, requests) =>
      if (iteration == max_iterations) {
        self ! End(iteration, 0)
      } else {
        val fields =  Seq("hash", "inputs", "outputs", "total")
        val requestedTransactions: mutable.Set[String] = txsPool.take(3)
        val requestUrl = txsBaseUrl + requestedTransactions.mkString(";")
        txsPool --= requestedTransactions
        println(s"Request Transactions $requestUrl")
        val r: Future[List[List[(Any, String)]]] = (workerTxToAd ? Request(requestUrl, fields, requestedTransactions.size == 1)).
          mapTo[List[List[(Any, String)]]]
        val newAddresses: mutable.Set[String] = new mutable.HashSet[String]()
        r.onComplete(x => {
          if (x.isSuccess) {
            x.get.foreach(e => {
              val hash: String = e.head._1.asInstanceOf[Option[String]].get
              val in: String = e(1)._1.asInstanceOf[Option[List[TransactionInputs]]].get.head.addresses.get.head
              val out: String = e(2)._1.asInstanceOf[Option[List[TransactionOutputs]]].get.head.addresses.get.head
              val tot: Long = e(3)._1.asInstanceOf[Option[Long]].get
              println(s"$in -> $hash ($tot) -> $out")
              // Add new nodes
              List(in, out).
                filter(!nodes.contains(_)).
                foreach(a => {
                  newAddresses += a
                  nodes(a) = (nodes.keySet.size + 1, None)
              })
              // Add edge
              edges(hash) = (nodes(in)_1, nodes(out)_1, tot)
            })
          } else {
            println("Completed with errors")
            println(x.failed.get)
          }
          Thread.sleep(1000)
          self ! ExtractTransactions(iteration + 1, requestedTransactions.size, newAddresses)
          println("_________________________")
        })
      }
    case ExtractTransactions(iteration, requests, newAddresses) =>
      if (iteration == max_iterations) {
        self ! End(iteration, 0)
      } else {
        val fields =  Seq("address", "txrefs", "balance")
        val requestedAddresses: Set[String] = newAddresses.take(3).toSet
        if (requestedAddresses.nonEmpty) {
          val requestUrl = addrBaseUrl + requestedAddresses.mkString(";") + "?limit=" + txRefLimit
          newAddresses --= requestedAddresses
          println(s"Request Addresses $requestUrl")
          val r: Future[List[List[(Any, String)]]] = (workerAdToTx ? Request(requestUrl, fields, requestedAddresses.size == 1)).
            mapTo[List[List[(Any, String)]]]
          r.onComplete(x => {
            if (x.isSuccess) {
              x.get.foreach(e => {
                val address: String = e.head._1.asInstanceOf[Option[String]].get
                val txrefs: List[String] = e(1)._1.asInstanceOf[Option[List[TransactionRef]]].get.map(_.tx_hash.get)
                val balance: Long = e(2)._1.asInstanceOf[Option[Long]].get
                println(s"$address: $txrefs, $balance")
                // Update pool of transactions
                txsPool ++= txrefs.toSet
                // Update new nodes
                nodes(address) = (nodes(address)._1, Some(balance))
              })
            } else {
              println("Completed with errors")
              println(x.failed.get)
            }
            Thread.sleep(1000)
            self ! ExtractAddresses(iteration + 1, requestedAddresses.size)
            println("_________________________")
          })
        } else {
          self ! ExtractAddresses(iteration + 1, 0)
        }
      }
    case End(iteration, requests) =>
      println(s"End ($iteration, $requests)")
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
  sealed trait TState
  abstract class State extends TState {
    def iteration: Int
    def requests: Int
  }

  case class ExtractAddresses(iteration: Int, requests: Int) extends State()
  case class ExtractTransactions(iteration: Int, requests: Int, newAddresses: mutable.Set[String]) extends State()
  case class End(iteration: Int, requests: Int) extends State()

}