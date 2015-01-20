name := "spark-playground"

version := "0.11"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")

// Base Spark-provided dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.2.1" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided")

// Extra libraries used in the playground
libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.3.7",
  "com.typesafe" % "config" % "1.2.1")

// Twitter integration
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1")

// Elasticsearch integration
libraryDependencies ++= Seq(
  ("org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0.BUILD-SNAPSHOT").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.slf4j", "slf4j-api")
)

// Kafka Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-kafka" % "1.2.1")

// HBase Dependencies
resolvers ++= Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Thrift" at "http://people.apache.org/~rawson/repo/")

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase" % "0.98.9-hadoop2",
  "org.apache.hbase" % "hbase-common" % "0.98.9-hadoop2",
  "org.apache.hbase" % "hbase-protocol" % "0.98.9-hadoop2",
  "org.apache.hbase" % "hbase-client" % "0.98.9-hadoop2",
  "org.apache.hbase" % "hbase-hadoop-compat" % "0.98.9-hadoop2",
  "org.apache.hbase" % "hbase-server" % "0.98.9-hadoop2" excludeAll ExclusionRule(organization = "org.mortbay.jetty") )

// Test-related libraries
libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.3.13" % "test")


scalariformSettings

// Skip tests when assembling fat JAR
test in assembly := {}

// Exclude jars that conflict with Spark (see https://github.com/sbt/sbt-assembly)
libraryDependencies ~= { _ map {
  case m if Seq("org.apache.hbase", "org.elasticsearch").contains(m.organization) =>
    m.exclude("commons-logging", "commons-logging").
      exclude("commons-collections", "commons-collections").
      exclude("commons-beanutils", "commons-beanutils-core").
      exclude("com.esotericsoftware.minlog", "minlog")
  case m => m
}}

initialCommands in console := """
  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.StreamingContext._
  import org.apache.spark.streaming.dstream._
  import org.apache.spark.streaming.kafka._
  import org.apache.spark.streaming.twitter._
  import org.elasticsearch.spark.rdd.EsSpark
  import org.elasticsearch.spark._
  import play.api.libs.json.Json
  import playground._
  import playground.model._
  val sparkConf = playground.DefaultConf("playground-console").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))
"""
