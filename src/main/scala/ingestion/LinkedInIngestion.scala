package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object LinkedInIngestion {
  def getProfiles(spark: SparkSession): DataFrame = {
    // TODO: implement ingestion from Kafka / Mock API
    spark.emptyDataFrame
  }
}
