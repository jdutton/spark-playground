# Spark Playground

This is a playground for experiments with Spark!  Who's got two thumbs and is excited...!?

## Getting Started

On a Mac, run `brew update` and `brew install apache-spark sbt` to install the latest version of
Spark and the Scala `sbt` build system.

To get started playing, run `spark-shell` and follow the
[Spark Quick Start](http://spark.apache.org/docs/latest/quick-start.html) guide for some examples of
simple interactive processing.

The Scala experiments code is located in the standard directory layout at `src/main/scala`.

To build the various experiments into a jar for submitting to spark:

```
$ sbt assembly
$ spark-submit --class playground.HardFeelings --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
```

Note that spark-submit will use the `HADOOP_CONF_DIR` environment variable to find HDFS.  To run
without HDFS, make sure this environment variable is *not set*.

## Live Tweets

To process live tweets, copy `src/main/resources/twitter.conf.example` to
`src/main/resources/twitter.conf` and modify the config to specify your Twitter API credentials.  If
you don't have Twitter API credentials, see
http://ampcamp.berkeley.edu/3/exercises/realtime-processing-with-spark-streaming.html for directions
how to obtain them.

To see live tweets printed out:

```
$ spark-submit --class playground.connector.Twitter --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
```


## Development

To develop in Eclipse (like the Scala IDE), initialize or update the Scala project by running:

```
$ sbt eclipse
```

## Testing

To run unit tests:

```
$ sbt test
```

## ToDo

1. Implement graphing library
2. Figure out general approach to impementing algorithms
3. Implement JSON library
