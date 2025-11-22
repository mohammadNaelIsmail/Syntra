import org.apache.spark.sql.SparkSession
import ingestion.LinkedInIngestion
import processing.{ProfileCleaner, SkillAnalyzer}
import storage.ElasticsearchWriter

object MainApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LinkedIn Pipeline Skeleton")
      .master("local[*]")
      .getOrCreate()
//testing
    val rawDF = LinkedInIngestion.getProfiles(spark)
    val cleanedDF = ProfileCleaner.clean(rawDF)
    val analyzedDF = SkillAnalyzer.analyze(cleanedDF)
    ElasticsearchWriter.write(analyzedDF)

    spark.stop()
  }
}
