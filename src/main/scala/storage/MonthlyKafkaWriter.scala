package storage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MonthlyKafkaWriter {

  def writeBatch(df: DataFrame): Unit = {
    val dfWithMonth =
      df.withColumn("month", date_format(col("last_update"), "yyyy-MM"))

    dfWithMonth
      .selectExpr(
        "CAST(person_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
      )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "linkedin_monthly_raw")
      .save()
  }
}
