package storage

import org.elasticsearch.spark._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object ElasticsearchProcessDataWriter {

  def write(jsonObject: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val df: DataFrame = Seq(jsonObject).toDF("raw_json")
      .select(
        from_json(
          col("raw_json"),
          spark.read.json(Seq(jsonObject).toDS).schema
        ).as("data")
      )
      .select("data.*")

    df.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", "localhost")
      .option("es.port", "9200")
      .option("es.resource", "people_monthly_analytics")
      .option("es.mapping.id", "month")
      .mode("append")
      .save()
  }
}
