package storage

import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

object ElasticsearchWriter {

  def write(df: DataFrame, index: String = "people"): Unit = {
    val esOptions = Map(
      "es.nodes" -> "localhost",
      "es.port" -> "9200",
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "15u7bMLAf=srLFvjJCeg"
    )

    df.saveToEs(s"$index", esOptions)
    println(s"Data successfully written to Elasticsearch index: $index")
  }
}
