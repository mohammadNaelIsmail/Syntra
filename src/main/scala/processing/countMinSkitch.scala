package processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object countMinSkitch {

  def countCompaniesCMSAsDF(df: DataFrame): DataFrame =
    df.withColumn("company", explode(col("companies.company_name")))
      .groupBy("company")
      .count()
      .withColumnRenamed("count", "estimated_count")
      .orderBy(desc("estimated_count"))

  def trendingSkillsCMS(df: DataFrame): DataFrame =
    df.withColumn("skill", explode(col("skills_after")))
      .groupBy("skill")
      .count()
      .withColumnRenamed("count", "estimated_count")
      .orderBy(desc("estimated_count"))
}
