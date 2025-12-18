package analytics

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.Level
import ingestion.KafkaMonthlyReader
import processing.{Best_Kperson_skills, Best_Kperson_company, localitySensitiveHashing}
import storage.ElasticsearchWriter
import org.apache.spark.sql.functions._

object MonthlyAnalyticsJob {

  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)


    val spark = SparkSession.builder()
      .appName("MonthlyAnalytics-Batch")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    // 1) اقرأ آخر شهر فقط (جاهز من الـ reader)
    val monthlyDF = KafkaMonthlyReader.read(spark, None).cache()

    // 2) تأكد أن في بيانات
    val cnt = monthlyDF.count()
    println(s"Monthly rows = $cnt")
    if (cnt == 0) {
      println("No data found for latest month. Exiting.")
      spark.stop()
      return
    }

    // 3) شهر آخر الداتا بشكل deterministic
    val latestMonth = monthlyDF.select(max(col("month"))).as[String].head()
    println(s"Processing month: $latestMonth")

    // -------- Top Skills --------
    val topSkills = Best_Kperson_skills
      .getTopKBySkills(monthlyDF, 10)
      .withColumn("month", lit(latestMonth))

    ElasticsearchWriter.writeBatch(
      topSkills,
      index = "people_top_skills",
      idCol = "person_id"
    )

    // -------- Top Companies --------
    val topCompanies = Best_Kperson_company
      .getTopKByCompanies(monthlyDF, 10)
      .withColumn("month", lit(latestMonth))

    ElasticsearchWriter.writeBatch(
      topCompanies,
      index = "people_top_companies",
      idCol = "person_id"
    )

    // -------- Similar People --------
    val similarPeople = localitySensitiveHashing
      .similarCareerPathsLSH_Local(monthlyDF)
      .withColumn("month", lit(latestMonth))

    ElasticsearchWriter.writeBatch(
      similarPeople,
      index = "people_similar",
      idCol = "other_person_id"
    )

    println("Done writing analytics indices to Elasticsearch.")
    spark.stop()
  }
}
