package playground.helper

import java.nio.file.Files
import java.net.InetSocketAddress
import org.apache.zookeeper.server.{ NIOServerCnxnFactory, ServerCnxnFactory, ZooKeeperServer }

object ZookeeperUnit {
  def apply(name: String, zkPort: Int) = new ZookeeperUnit(name = name, zkPort = zkPort)
}

class ZookeeperUnit(val name: String, val zkPort: Int) {
  private val zkFactory = new NIOServerCnxnFactory()

  def start() {
    val logDir = Files.createTempDirectory("zk-log").toFile()
    logDir.deleteOnExit()
    val snapDir = Files.createTempDirectory("zk-snap").toFile()
    snapDir.deleteOnExit()
    val tickTime = 2000 // Zookeeper default - used for heartbeating - minimum session timeout is 2 * tickTime
    val zkServer = new ZooKeeperServer(snapDir, logDir, tickTime)
    zkFactory.configure(new InetSocketAddress("localhost", zkPort), 16)
    zkFactory.startup(zkServer)
    println(s"ZookeeperUnit($name) STARTED (zkPort=$zkPort)")
  }

  def stop() {
    zkFactory.shutdown()
    println(s"ZookeeperUnit($name) STOPPED (zkPort=$zkPort)")
  }
}
