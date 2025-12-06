package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object LinkedInIngestion {

  def getProfiles(spark: SparkSession): DataFrame = {
    val kafkaDF: DataFrame = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "host1:9092")
      .option("subscribe", "linkedin_topic")
      .load()
      .selectExpr("CAST(value AS STRING) as json_str")

    kafkaDF
  }

}
