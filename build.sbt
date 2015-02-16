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
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided")

// Extra libraries used in the playground
libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.3.7")

// Elasticsearch integration
libraryDependencies ++= Seq(
  ("org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0.Beta3").
    exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.slf4j", "slf4j-api").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-collections", "commons-collections")
    exclude("commons-logging", "commons-logging")
)

// Test-related libraries
libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.3.13" % "test")


scalariformSettings

// Skip tests when assembling fat JAR
test in assembly := {}

initialCommands in console := """
  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import playground._
  val sparkConf = playground.DefaultConf("playground-console").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
"""
