package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LinkedInIngestion {

  def getProfiles(spark: SparkSession , topic: String = "test" ): DataFrame = {
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe",topic)
      .load()
      .selectExpr("CAST(value AS STRING) as json_str")

    kafkaDF
  }
  val schema = new StructType()
    .add("id", StringType, true)
    .add("name", StringType, true)
    .add("skills_before", ArrayType(StringType), true)
    .add("skills_after", ArrayType(StringType), true)
    .add("new_skill", StringType, true)
    .add("date", DateType, true)
    .add("companies", ArrayType(
      new StructType()
        .add("company_name", StringType, true)
        .add("date_from", DateType, true)
        .add("date_to", DateType, true)
             ), true)
    .add("profile_text",StringType)
    .add("current_status",StringType)
    .add("last_update",DateType)

  def parseProfiles(df: DataFrame): DataFrame = {
    df.select(from_json(col("json_str"), schema).as("data"))
      .select("data.*")
  }

  def explodeCompanies(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.withColumn("company", explode_outer($"companies"))
      .select(
        $"id",
        $"name",
        $"skills_before",
        $"skills_after",
        $"new_skill",
        $"date",
        $"company.company_name",
        $"company.date_from",
        $"company.date_to",
        $"profile_text",
        $"current_status",
        $"last_update"
      )
  }

}

