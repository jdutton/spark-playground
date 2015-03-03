package playground.connector

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._
import play.api.libs.json.{ Json, Reads, Writes }
import scala.collection.immutable._
import scala.reflect.ClassTag

/**
 * Elasticsearch connector helper functions.
 *
 * For more info, see http://www.elasticsearch.org/guide/en/elasticsearch/hadoop/master/spark.html
 */
object Elasticsearch {

  /**
   * Write an RDD[A] to Elasticsearch, converting A to Json.
   * @param resource is the index and document type, for example "es-index/es-document-type"
   * @param config takes a Map of Elasticsearch Spark config options, such as Map("es.mapping.id" -> "objIdField")
   */
  def write[A](rdd: RDD[A], resource: String, config: Map[String, String] = Map())(implicit tjs: Writes[A]): Unit = {
    val jsonStrings = rdd.map(Json.toJson(_).toString)
    jsonStrings.saveJsonToEs(resource, config)
  }

  /**
   * Read an RDD[A] from Elasticsearch, converting ES Json to A objects.
   * @param resource is the index and document type, for example "es-index/es-document-type"
   * @param query optionally restricts the returned objects, based on Elasticsearch query
   */
  def read[A: ClassTag](sc: SparkContext, resource: String, query: String = "")(implicit tjs: Reads[A]): RDD[A] = {
    sc.esJsonRDD(resource, query).values.flatMap(Json.parse(_).asOpt[A])
  }

}