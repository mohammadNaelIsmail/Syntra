import config.AppConfig
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import storage.ElasticsearchWriter
import ingestion.LinkedInIngestion
import analytics.MonthlyAnalyticsJob
import storage.ElasticsearchProcessDataWriter

object MainForStreamData {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LinkHarvester-Stream")
      .master("local[*]")
      .getOrCreate()

    val rawDF      = LinkedInIngestion.getProfiles(spark, AppConfig.Kafka.topic)
    val parsedDF   = LinkedInIngestion.parseProfiles(rawDF)
    val explodedDF = LinkedInIngestion.explodeCompanies(parsedDF)

    val snapshotQuery = ElasticsearchWriter.writeStream(explodedDF, AppConfig.Elasticsearch.snapshotIndex)

    val analyticsQuery = parsedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (batchDF.isEmpty) return

        implicit val ss: SparkSession = batchDF.sparkSession

        val months = batchDF
          .withColumn("month", date_format(to_date(col("last_update")), "yyyy-MM"))
          .select("month")
          .distinct()
          .collect()
          .map(_.getString(0))
          .filter(_ != null)

        months.foreach { month =>
          val monthDF = batchDF.filter(
            date_format(to_date(col("last_update")), "yyyy-MM") === month
          )
          val analyticsJson = MonthlyAnalyticsJob.colectoor(monthDF)
          println(s"\n----- ANALYTICS: $month -----")
          ElasticsearchProcessDataWriter.write(analyticsJson)
        }
      }
      .option("checkpointLocation", AppConfig.Checkpoints.analytics)
      .start()

    snapshotQuery.awaitTermination()
    analyticsQuery.awaitTermination()
  }
}
