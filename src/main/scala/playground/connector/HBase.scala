package playground.connector

import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor, KeyValue }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Result }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat, TableInputFormat, TableOutputFormat }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object HBase {
  def apply(hbaseConfigFilename: String = "") = new HBase(hbaseConfigFilename)
}
class HBase(hbaseConfigFilename: String) {

  def createConfiguration(filename: String = hbaseConfigFilename) = {
    val configFilename = if (filename.nonEmpty) filename else System.getenv("HBASE_HOME") + "/conf/hbase-site.xml"
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.addResource(configFilename)
    //.set("hbase.zookeeper.quorum", "localhost")
    //.set("hbase.zookeeper.property.clientPort", "2181")
    //.set("hbase.mapreduce.inputtable", "spark-playground")
    hbaseConfiguration
  }

  /**
   * Create the specified table and column families.
   * Return true if the table was created, false if the table already existed
   */
  def createTable(tableName: String, columnFamilies: Seq[String]): Boolean = {
    val admin = new HBaseAdmin(createConfiguration())
    val table = new HTableDescriptor(tableName)
    columnFamilies.foreach { columnFamily =>
      val colFam = new HColumnDescriptor(columnFamily.getBytes())
      table.addFamily(colFam)
    }

    try {
      admin.createTable(table)
      true
    } catch {
      case err: org.apache.hadoop.hbase.TableExistsException => false
    }
  }

  // See http://www.openkb.info/2015/01/how-to-use-scala-on-spark-to-load-data.html#.VOK6HlPF_Mc
  def write(rdd: RDD[KeyValue], tableName: String) {
    val config = createConfiguration()
    config.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val table = new HTable(config, tableName)
    val job = Job.getInstance(config)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", "hbase-tmp-load-dir")
    HFileOutputFormat.configureIncrementalLoad(job, table)

    val hbaseOutputRDD = rdd.map { kv => (new ImmutableBytesWritable(kv.getKey), kv) }

    // Directly bulk load to Hbase/MapRDB tables.
    hbaseOutputRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def write(rdd: RDD[(String, String)], tableName: String, columnFamily: String, column: String) {
    val kvOutputRDD = rdd.sortBy(_._1, ascending = true).map {
      case (keyString, value) =>
        new KeyValue(keyString.getBytes, columnFamily.getBytes, column.getBytes, value.getBytes)
    }
    write(kvOutputRDD, tableName)
  }

  def read(sc: SparkContext, tableName: String, columnFamily: String, column: String): RDD[Array[Byte]] = {
    val config = createConfiguration()
    config.set(TableInputFormat.INPUT_TABLE, tableName)

    val rdd = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd
      .map(tuple => tuple._2)
      .map(result => result.getValue(columnFamily.getBytes, column.getBytes))
  }

}