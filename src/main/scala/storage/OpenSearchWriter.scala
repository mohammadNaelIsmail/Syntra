package storage


import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}


object OpenSearchWriter {

  def write(df: DataFrame, index: String = "people"): Unit = {
    val osOptions = Map(
      "opensearch.nodes" -> "localhost",
      "opensearch.port" -> "9200"
    )
    val query: StreamingQuery = df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        for (row <- batchDF.collect()) {
          val json = row.mkString(", ")
          println(s"Indexed row to OpenSearch: $json")
        }

      batchDF.write
      .format("opensearch")
      .options(osOptions)
      .option("resource", index)
      .mode("append")
      .save()

      } .start()

    //df.saveToOpenSearch(s"$index", osOptions)
    //println(s"Data successfully written to OpenSearch index: $index")

  }
}
