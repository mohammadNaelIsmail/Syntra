package storage
import config.AppConfig

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object ElasticsearchProcessDataWriter {

  def write(jsonObject: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    spark.read.json(Seq(jsonObject).toDS)
      .write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes",       AppConfig.Elasticsearch.nodes)
      .option("es.port",        AppConfig.Elasticsearch.port)
      .option("es.resource",    AppConfig.Elasticsearch.analyticsIndex)
      .option("es.mapping.id",  "month")
      .mode("append")
      .save()
  }
}
