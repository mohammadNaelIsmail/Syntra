package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.CountMinSketch

//الاستدعاء
//    val companiesCountDF = countCompaniesCMSAsDF(df1000)
//    companiesCountDF.show(false)
//    val trendingDF = trendingSkillsCMS(df)
//    trendingDF.show(false)

object countMinSkitch {

  // ======================================
  // 1)عدد الاشخاص في كل شركة Google
  // ======================================
  //الاستدعاء

  def countCompaniesCMSAsDF(df: DataFrame): DataFrame = {

    // 1) تفجير أسماء الشركات
    val explodedDF = df
      .withColumn("company", explode(col("companies.company_name")))
      .select("company")

    // 2) بناء Count-Min Sketch
    val cms = explodedDF
      .stat
      .countMinSketch(
        col("company"),
        eps = 0.001,
        confidence = 0.99,
        seed = 1
      )

    // 3) استخراج الشركات بدون تكرار
    val uniqueCompaniesDF = explodedDF.distinct()

    // 4) تقدير العدد لكل شركة
    val estimateUDF = udf((c: String) => cms.estimateCount(c))

    uniqueCompaniesDF
      .withColumn("estimated_count", estimateUDF(col("company")))
      .orderBy(desc("estimated_count"))
  }




  // ======================================
  // 2) Count-Min Sketch: Trending Skills
  // ======================================
  //input the date and skill output skill and frecuancy


  def trendingSkillsCMS(df: DataFrame): DataFrame = {

    // 1) explode المهارات
    val explodedDF = df
      .withColumn("skill", explode(col("skills_after")))
      .select(col("last_update"), col("skill"))

    // 2) بناء Count-Min Sketch
    val cms: CountMinSketch =
      explodedDF.stat.countMinSketch(
        col("skill"),
        eps = 0.001,
        confidence = 0.99,
        seed = 42
      )

    // 3) الشركات/المهارات بدون تكرار
    val distinctSkillsDF = explodedDF.select("last_update", "skill").distinct()

    // 4) UDF للاستعلام من الـ CMS
    val estimateUDF = udf((s: String) => cms.estimateCount(s))

    // 5) بناء DF النهائي
    distinctSkillsDF
      .withColumn("estimated_count", estimateUDF(col("skill")))
      .orderBy(col("last_update"), desc("estimated_count"))
  }



}