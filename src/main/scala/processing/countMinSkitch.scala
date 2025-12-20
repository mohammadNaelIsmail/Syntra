package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.CountMinSketch

object countMinSkitch {

  def countCompaniesCMSAsDF(df: DataFrame): DataFrame = {

    val explodedDF = df
      .withColumn("company", explode(col("companies.company_name")))
      .select("company")

    val cms = explodedDF
      .stat
      .countMinSketch(
        col("company"),
        eps = 0.001,
        confidence = 0.99,
        seed = 1
      )


    val uniqueCompaniesDF = explodedDF.distinct()


    val estimateUDF = udf((c: String) => cms.estimateCount(c))

    uniqueCompaniesDF
      .withColumn("estimated_count", estimateUDF(col("company")))
      .orderBy(desc("estimated_count"))
  }



  def trendingSkillsCMS(df: DataFrame): DataFrame = {


    val explodedDF = df
      .withColumn("skill", explode(col("skills_after")))
      .select(col("last_update"), col("skill"))


    val cms: CountMinSketch =
      explodedDF.stat.countMinSketch(
        col("skill"),
        eps = 0.001,
        confidence = 0.99,
        seed = 42
      )


    val distinctSkillsDF = explodedDF.select("last_update", "skill").distinct()


    val estimateUDF = udf((s: String) => cms.estimateCount(s))


    distinctSkillsDF
      .withColumn("estimated_count", estimateUDF(col("skill")))
      .orderBy(col("last_update"), desc("estimated_count"))
  }



}