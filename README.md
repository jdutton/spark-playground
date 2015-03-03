# Spark Playground

[![Build Status](https://travis-ci.org/jdutton/spark-playground.svg?branch=master)](https://travis-ci.org/jdutton/spark-playground)

This is a playground for experiments with Spark!  Who's got two thumbs and is excited...!?

## Getting Started

On a Mac, run `brew update` and `brew install apache-spark sbt` to install the latest version of
Spark and the Scala `sbt` build system.

To get started playing, run `spark-shell` and follow the
[Spark Quick Start](http://spark.apache.org/docs/latest/quick-start.html) guide for some examples of
simple interactive processing.

The Scala experiments code is located in the standard directory layout at `src/main/scala`.

To build the various experiments into a jar for submitting to spark, run `sbt assembly`.  For example:

```
$ sbt assembly
$ spark-submit --class playground.HardFeelings --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
```

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

## Elasticsearch

To enable Elasticsearch in the playground, make sure Elasticsearch is running locally (Elasticsearch
REST API should be available at http://localhost:9200), then you can enable Elasticsearch support
by adding `--conf spark.playground.es.enabled=true` to the `spark-submit` command arguments.
See examples below.

### Kafka

To enable Kafka in the playground, make sure Kafka is running locally.  Kafka broker expected to be
on `localhost:9092` and zookeeper is expected to be on `localhost:2181

See http://www.michael-noll.com/blog/2014/10/01/kafka-spark-streaming-integration-example-tutorial/
for a good guide on Spark/Kafka integration.

To listen in externally to the `MericaTweets` feed from `MericaStreaming`, optionally from the
beginning of the queue:

```
$ kafka-console-consumer.sh --zookeeper localhost:2181 --topic MericaTweets [--from-beginning]
```

## HDFS

To output to HDFS (or simulated HDFS via local files), add `--conf spark.playground.es.enabled=true`
to the `spark-submit` command arguments.

Note that spark-submit will use the `HADOOP_CONF_DIR` environment variable to find HDFS.  When HDFS
is not properly setup, make sure this environment variable is *not set*.  Then Spark will write to
local files rather than HDFS.

## Streaming Tweet Processing

To run streaming tweet processing, you can enable or disable various external datastores, for example:

```
$ spark-submit --class playground.MericaStreaming --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
$ spark-submit --conf spark.playground.kafka.enabled=true --class playground.MericaStreaming --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
$ spark-submit --conf spark.playground.kafka.enabled=true --conf spark.playground.es.enabled=true --class playground.MericaStreaming --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
```

To save the tweets for offline batch processing (see below), run with `--conf
spark.playground.hdfs.enabled=true`, e.g:

```
$ spark-submit --conf spark.playground.hdfs.enabled=true --class playground.MericaStreaming --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
```

Note that any combination of the above `--conf` external datastores is supported.


## Batch Tweet Processing

To do batch sentiment analysis on the tweets saved from the MericaStreaming processing described
above, run any of the following:

```
$ spark-submit --class playground.Merica --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
$ spark-submit --conf spark.playground.es.enabled=true --class playground.Merica --master local[4] target/scala-2.10/spark-playground-assembly-*.jar
```

## Development

Just like any other SBT project, to develop in Eclipse (like the Scala IDE), initialize or update the Scala project by running:

```
$ sbt eclipse
```

## Testing

Just like any other SBT project, to run unit tests:

```
$ sbt test
```
