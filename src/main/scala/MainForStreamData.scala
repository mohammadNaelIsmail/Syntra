//-------MAIN FOR STREAM DATA ----------------------
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import storage.{ElasticsearchWriter, MonthlyKafkaWriter}
import ingestion.LinkedInIngestion

object MainForStreamData {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MainApp")
      .master("local[]")
      .getOrCreate()

    val rawDF = LinkedInIngestion.getProfiles(spark, "test")
    val parsedDF = LinkedInIngestion.parseProfiles(rawDF)

    parsedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"\n========= BATCH $batchId =========")
        batchDF.show(false)
      }
      .option("checkpointLocation", "/tmp/checkpoint/print_stream")
      .start()

    val explodedDF = LinkedInIngestion.explodeCompanies(parsedDF)
    val query = ElasticsearchWriter.writeStream(explodedDF, "people_snapshot")
    query.awaitTermination()
  }
}















