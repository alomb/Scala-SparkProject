import org.apache.spark.sql.SparkSession
import java.net.{HttpURLConnection, URL}

object HelloWorld {

  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  def get(url: String, connectTimeout: Int = 5000, readTimeout: Int = 5000, requestMethod: String = "GET"): String = {
    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close()
    content
  }


  def main(args: Array[String]) {
    println("Program starts")

    val path = "test_ether.json"
    val txs = Array(
      "161af0b54c17b64963947bb79c7f4de6c0aacf6b87d6e94f807c7d399e4bcf22",
      "9625cb071eb86157c524b08174acfe2132f052048a715e662a820081fc677842",
      "d27fe8592b0e6a73eeb4252a6f9d5d0071f9c8b659c3e36d4e479884094705e4",
      "d076fdd133520cb2103790e4838a93ac55d6a548b0399c4b84411fafc5330766",
      "c8932e85e98d1ff8b7828efb868547ff4f7793a83ce858afc7362d9e3cbf717f",
      "2135843e23b1c0476313ccaa59418365880f97c849bac3e14ce98aa576d166e4",
      "d2a211d54ec76756325bc84f66b7f8296bd5c421e38bad503875182a9cf054e2",
      "ef4c6566ef6703e03dcc94f97d9c36b5d37d58a3091f5218d0a95cbc54cc53ba",
      "3397a0d30e3a98c426e3d2673fe00bb32c4b9d8f096f1b4d8893bb6750c912b5",
      "9040c929c046f72bb1c32f0a82278d44ca2cd1e6ced9db78347d58e8587a592d",
      "f77ef70dba47135fa2e594e70d097dc804f594f68cf65d82680b094abeaac616",
      "97f0e7a04542be2af6d329b20b3e8cd7a2132dfcc55f22051df12dfba3a0e911",
      "4acb439111e2329ffce7fee4b2ba316b1bdf8c1f988fbbdf9942fa31a606449b",
      "5a0d3d0a804bce515dd7e3506e657e9a3b91c72c9641a597ed795d1c9a0d152b")

    /*
    def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
      val reader = spark.read
      Option(schema).foreach(reader.schema)
      reader.json(spark.sparkContext.parallelize(Array(json)).toDS())
    }
    */

    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("Test")
      .getOrCreate()
    import spark.implicits._

    val queryResult = spark.sparkContext.parallelize(txs).take(2).
      map(x => get("https://api.blockcypher.com/v1/eth/main/txs/".concat(x))).
      map(x => spark.read.json(Seq(x).toDS)).
      map(x => x.select($"hash", $"inputs.addresses" as 'in, $"outputs.addresses" as 'out).collect()(0)).
      map(x => (x.getString(0), x.getSeq(1).flatten.head, x.getSeq(2).flatten.head))

    queryResult.foreach(println(_))

    println("Program ends")
  }
}