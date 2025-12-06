package storage

import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.SparkSession

object ElasticsearchWriter {

  def write(df: DataFrame, index: String = "people"): Unit = {
    val esOptions = Map(
      "es.nodes" -> "localhost",
      "es.port" -> "9200",
      "es.net.ssl" -> "true",
      "es.net.ssl.cert.allow.self.signed" -> "true",
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "1=UgWUJYXrYnBIziNxWl",
      "es.mapping.id" -> "id"
    )

    df.show(false)
    df.saveToEs(s"$index/_doc", esOptions)
    println(s"Data successfully written to Elasticsearch index: $index")
  }
}
