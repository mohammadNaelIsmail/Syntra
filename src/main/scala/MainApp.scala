import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import scala.io.StdIn
import storage.OpenSearchWriter
import processing.Best_Kperson_skills
import processing.Best_Kperson_company
import processing.localitySensitiveHashing
import ingestion.LinkedInIngestion
object MainApp {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MainApp")
      .master("local[*]")
      .getOrCreate()

val rawDF = LinkedInIngestion.getProfiles(spark,"test")

    rawDF
      .writeStream
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
    val parsedD = LinkedInIngestion.parseProfiles(rawDF)
    val exeplodedDF=LinkedInIngestion.explodeCompanies(parsedD)






    spark.stop()
    //OpenSearchWriter.write(df, "people")
  }
}
