package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LinkedInIngestion {

  def getProfiles(spark: SparkSession, topic: String = "test"): DataFrame = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(value AS STRING) as json_str")

    kafkaDF
  }

  val schema = new StructType()
    .add("person_id", StringType)
    .add("name", StringType)
    .add("skills_before", ArrayType(StringType))
    .add("skills_after", ArrayType(StringType))
    .add("new_skill", StringType)
    .add("date", StringType)
    .add("last_update", StringType)
    .add("profile_text", StringType)
    .add("current_status", StringType)
    .add(
      "companies",
      ArrayType(
        new StructType()
          .add("company_name", StringType)
          .add("date_from", StringType)
          .add("date_to", StringType)
      )
    )

  def parseProfiles(df: DataFrame): DataFrame = {
    df.select(from_json(col("json_str"), schema).as("data"))
      .select(
        col("data.person_id"),
        col("data.name"),
        col("data.profile_text"),
        col("data.current_status"),
        col("data.skills_before"),
        col("data.skills_after"),
        col("data.new_skill"),
        to_date(col("data.last_update"), "yyyy-MM-dd").as("last_update"),
        col("data.companies")
      )
  }


  def explodeCompanies(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.withColumn("company", explode_outer($"companies"))
      .select(
        $"person_id",
        $"name",
        $"profile_text",
        $"current_status",
        $"skills_before",
        $"skills_after",
        $"new_skill",
        $"last_update",
        $"company.company_name".as("company_name"),
        $"company.date_from",
        $"company.date_to"
      )
  }
}
