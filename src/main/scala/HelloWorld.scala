import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]) {
    println("Program starts")
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("HelloWorld")
      .getOrCreate()
    val textFile = spark.read.textFile("README.md")
    println("Lines: " + textFile.count())
    spark.close()
    println("Program ends")
  }
}