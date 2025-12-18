package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object KafkaMonthlyReader {

  def read(spark: SparkSession, month: Option[String] = None): DataFrame = {
    val kafkaDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "linkedin_monthly_raw")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), LinkedInIngestion.schema).as("data"))
      .select("data.*")
      .withColumn("month", date_format(col("last_update"), "yyyy-MM"))

    month match {
      case Some(m) => kafkaDF.filter(col("month") === m)
      case None =>
        val latestMonth = kafkaDF.select(max("month")).as[String].first()
        kafkaDF.filter(col("month") === latestMonth)
    }
  }
}
