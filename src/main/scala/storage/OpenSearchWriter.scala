package storage

import org.apache.spark.sql.DataFrame
import org.opensearch.spark.sql._

object OpenSearchWriter {

  def write(df: DataFrame, index: String = "people"): Unit = {
    val osOptions = Map(
      "opensearch.nodes" -> "localhost",
      "opensearch.port" -> "9200"
    )

    df.saveToOpenSearch(s"$index", osOptions)
    println(s"Data successfully written to OpenSearch index: $index")
  }
}
