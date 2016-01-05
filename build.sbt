name := "spark-playground"

version := "0.11"

scalaVersion := "2.11.7"

val sparkVers = "1.6.0"

resolvers ++= Seq(
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")

// Base Spark-provided dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVers % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVers % "provided")

// Extra libraries used in the playground
libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.3.7",
  "com.typesafe" % "config" % "1.2.1")

// Twitter integration
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVers)

// Elasticsearch integration
libraryDependencies ++= Seq(
  ("org.elasticsearch" %% "elasticsearch-spark" % "2.1.2").
    exclude("com.google.guava", "guava").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.slf4j", "slf4j-api")
)

// Kafka Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVers,
  "org.apache.kafka" % "kafka-clients" % "0.8.2.1")

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

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

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
