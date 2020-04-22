lazy val root = (project in file("."))
  .settings(
    name := "Scala-SparkProject",
    scalaVersion := "2.11.12",
    version := "0.1",
    mainClass in Compile := Some("EthereumMain")
  )

val sparkVersion = "2.4.4"
val akkaVersion = "10.1.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "com.typesafe.akka" %% "akka-http" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % "2.5.26",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
)
