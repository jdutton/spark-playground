import AssemblyKeys._

name := "spark-playground"

version := "0.11"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.0.1" % "provided",
    "org.apache.spark" %% "spark-streaming-twitter" % "1.0.1" % "provided",
    "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided",
    "org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.1.0.BUILD-SNAPSHOT",
    "com.typesafe.play" %% "play-json" % "2.2.2",
    "org.specs2" %% "specs2" % "2.3.13" % "test"
)



resolvers ++= Seq(
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

assemblySettings
