package playground

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

object DefaultConf {
  def apply(name: String, master: String = "") = {
    val conf = new org.apache.spark.SparkConf(loadDefaults = true).setAppName(name)
    if (master.nonEmpty)
      conf.setMaster(master)
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("es.index.auto.create", "true")
    conf
  }
}
