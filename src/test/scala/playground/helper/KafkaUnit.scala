package playground.helper

import java.nio.file.Files
import java.util.Properties
import kafka.admin.TopicCommand
import kafka.consumer._
import kafka.serializer.StringDecoder
import kafka.server.{ KafkaConfig, KafkaServerStartable }
import kafka.utils.VerifiableProperties
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.{ ExecutionContext, Future }

object KafkaUnit {
  def getOpenPort: Int = {
    val sock = new java.net.ServerSocket(0)
    val port = sock.getLocalPort
    sock.close
    port
  }

  def apply(name: String, brokerPort: Int = 0, zkPort: Int = 0): KafkaUnit = {
    val realBrokerPort = if (brokerPort > 0) brokerPort else getOpenPort
    val realZkPort = if (zkPort > 0) zkPort else getOpenPort
    new KafkaUnit(name = name, brokerPort = realBrokerPort, zkPort = realZkPort)
  }
}

class KafkaUnit(val name: String, val brokerPort: Int, val zkPort: Int) {
  assert(brokerPort != zkPort)

  val brokers = Seq(s"localhost:$brokerPort")
  val zkHosts = Seq(s"localhost:$zkPort")

  val zookeeper = ZookeeperUnit(name = name, zkPort = zkPort)

  // Create broker at start()
  private lazy val broker = {
    val logDir = Files.createTempDirectory("kafka").toFile()
    logDir.deleteOnExit()

    val config = new Properties()
    config.put("zookeeper.connect", s"localhost:$zkPort")
    config.put("broker.id", 1.toString)
    config.put("host.name", "localhost")
    config.put("port", brokerPort.toString)
    config.put("log.dir", logDir.getAbsolutePath)

    new KafkaServerStartable(new KafkaConfig(config))
  }

  def start() {
    zookeeper.start()
    broker.startup()
    println(s"KafkaUnit($name) STARTED (brokerPort=$brokerPort, zkPort=$zkPort)")
  }

  def stop() {
    broker.shutdown()
    zookeeper.stop()
    println(s"KafkaUnit($name) STOPPED (brokerPort=$brokerPort, zkPort=$zkPort)")
  }

  def topicCmd(args: Array[String]) {
    TopicCommand.main(args)
    println(s"KafkaUnit($name) Topic ${args.mkString(" ")}")
  }

  def createTopic(topicName: String, numPartitions: Int = 1) {
    topicCmd(Array[String]("--create", "--topic", topicName, "--zookeeper", s"localhost:$zkPort", "--replication-factor", "1", "--partitions", numPartitions.toString))
  }

  def listTopics() {
    topicCmd(Array[String]("--list", "--zookeeper", s"localhost:$zkPort"))
  }

  val stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()))

  def readMessagesBlocking(topicName: String, waitForSecs: Int = 2): Seq[(String, String)] = {
    val config = new Properties()
    config.put("zookeeper.connect", zkHosts.mkString(","))
    config.put("group.id", "KafkaUnit")
    config.put("socket.timeout.ms", 500.toString)
    config.put("consumer.id", "unit")
    config.put("auto.offset.reset", "smallest")
    config.put("consumer.timeout.ms", (waitForSecs * 1000).toString) // This is what aborts this read

    val connector = Consumer.create(new ConsumerConfig(config))
    val kafkaStreamsByTopic = connector.createMessageStreams[String, String](Map(topicName -> 1), stringDecoder, stringDecoder)
    val kafkaStreams = kafkaStreamsByTopic.get(topicName)
    val kafkaStream = kafkaStreams.get(0)
    val messages = collection.mutable.Queue[(String, String)]()
    try {
      // kafkaStream never ends, so use foreach() and wait for exception - don't map() over it
      kafkaStream.foreach(msg => messages += (msg.key -> msg.message))
    } catch {
      case _: ConsumerTimeoutException => ()
    }
    messages
  }

  implicit val ec = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))
  def readMessages(topicName: String, waitForSecs: Int = 2): Future[Seq[(String, String)]] = {
    Future { readMessagesBlocking(topicName, waitForSecs) }
  }

}
