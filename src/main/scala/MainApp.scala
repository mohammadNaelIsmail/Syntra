import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import storage.ElasticsearchWriter
import ingestion.LinkedInIngestion
object MainApp {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MainApp")
      .master("local[*]")
      .getOrCreate()
    val rawDF = LinkedInIngestion.getProfiles(spark,"test")
    val parsedDF = LinkedInIngestion.parseProfiles(rawDF)
    val explodedDF = LinkedInIngestion.explodeCompanies(parsedDF)
    ElasticsearchWriter.writeStream(explodedDF,"people_snapshot")
  }
}
