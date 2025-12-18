package analytics

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.Level
import ingestion.KafkaMonthlyReader
import processing.Best_Kperson_skills
import processing.Best_Kperson_company
import processing.localitySensitiveHashing
import storage.ElasticsearchWriter
import org.apache.spark.sql.functions._

object MonthlyAnalyticsJob {

  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MonthlyAnalytics-Auto")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val monthlyDF = KafkaMonthlyReader.read(spark, None).cache()
    val latestMonth = monthlyDF.select("month").as[String].first()
    println(s"Processing latest month: $latestMonth")

    val topSkills = Best_Kperson_skills
      .getTopKBySkills(monthlyDF, 10)
      .withColumn("month", lit(latestMonth))

    ElasticsearchWriter.writeBatch(
      topSkills,
      index = "people_top_skills",
      idCol = "person_id"
    )

    val topCompanies = Best_Kperson_company
      .getTopK(monthlyDF, 10)
      .withColumn("month", lit(latestMonth))

    ElasticsearchWriter.writeBatch(
      topCompanies,
      index = "people_top_companies",
      idCol = "person_id"
    )

    val similarPeople =
      localitySensitiveHashing.similarCareerPathsLSH_Local(monthlyDF)
        .withColumn("month", lit(latestMonth))

    ElasticsearchWriter.writeBatch(
      similarPeople,
      index = "people_similar",
      idCol = "other_person_id"
    )

    spark.stop()
  }
}
