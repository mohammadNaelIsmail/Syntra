package storage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MonthlyKafkaWriter {


  def writeStreamByMonth(
                          df: DataFrame,
                          bootstrapServers: String,
                          dateCol: String = "last_update",
                          topicPrefix: String = ""
                        ) = {

    df
      .withColumn(
        "month",
        date_format(to_date(col(dateCol)), "yyyy-MM")
      )
      .selectExpr(
        "CAST(person_id AS STRING) AS key",
        "to_json(struct(*)) AS value",
        "month"
      )
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val months = batchDF.select("month").distinct().collect().map(_.getString(0))

        months.foreach { m =>
          val topicName =
            if (topicPrefix.isEmpty) m else s"$topicPrefix-$m"

          batchDF
            .filter(col("month") === m)
            .drop("month")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("topic", topicName)
            .save()
        }
      }
      .option("checkpointLocation", "/tmp/checkpoint/kafka_monthly_router")
      .start()
  }
}

//
//val query =
//  MonthlyKafkaWriter.writeStreamByMonth(
//    df = parsedDF,
//    bootstrapServers = "localhost:9092",
//    dateCol = "last_update",
//    topicPrefix = "people"
//  )
//
//query.awaitTermination()