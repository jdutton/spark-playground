name := "spark-playground"

version := "0.11"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")

// Base Spark-provided dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided")

// Extra libraries used in the playground
libraryDependencies ++= Seq(
  "org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.1.0.BUILD-SNAPSHOT",
  "com.typesafe.play" %% "play-json" % "2.3.7")

// Test-related libraries
libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.3.13" % "test")


scalariformSettings
 
initialCommands in console := """
  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import playground._
  val sparkConf = playground.DefaultConf("playground-console").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
"""
