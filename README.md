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
$ sbt package
$ spark-submit --class playground.HardFeelings --master local[4] target/scala-2.10/spark-playground_*.jar
```

## Development

To develop in Eclipse (like the Scala IDE), initialize or update the Scala project by running:

```
$ sbt eclipse
```
