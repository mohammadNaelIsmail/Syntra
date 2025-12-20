package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import analytics.MonthlyAnalyticsJob
import storage.ElasticsearchProcessDataWriter
object ProcessDataReader {

  def processDataReader(
                         spark: SparkSession,
                         bootstrapServers: String,
                         topicPrefix: String
                       ): Unit = {

    val streamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribePattern", s"$topicPrefix-\\d{4}-\\d{2}")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr(
        "CAST(value AS STRING) AS json_value",
        "topic",
        "timestamp"
      )
      .withColumn("data", from_json(col("json_value"), peopleSchema))
      .select("topic", "timestamp", "data.*")

    val query = streamDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        implicit val spark: SparkSession = batchDF.sparkSession

        val topics = batchDF
          .select("topic")
          .distinct()
          .collect()
          .map(_.getString(0))

        topics.foreach { topicName =>
          val topicDF = batchDF.filter(col("topic") === topicName)
          val jsonopject = MonthlyAnalyticsJob.colectoor(topicDF)
          println(s"\n----- MONTH: $topicName -----")
          println(jsonopject)
          println("-----------------------------\n")
          ElasticsearchProcessDataWriter.write(jsonopject)
        }
      }
      //.option("checkpointLocation", "C:/tmp/checkpoints/people_stream")
      .start()

    query.awaitTermination()
  }

  private val peopleSchema: StructType = StructType(Array(
    StructField("person_id", StringType),
    StructField("name", StringType),
    StructField("profile_text", StringType),
    StructField("current_status", StringType),
    StructField("skills_before", ArrayType(StringType)),
    StructField("skills_after", ArrayType(StringType)),
    StructField("last_update", StringType),
    StructField("month", StringType),
    StructField(
      "companies",
      ArrayType(
        StructType(Array(
          StructField("company_name", StringType),
          StructField("date_from", StringType),
          StructField("date_to", StringType)
        ))
      )
    )
  ))

}

