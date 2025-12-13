import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.CountMinSketch
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator

object countMinSkitch {

  // ======================================
  // 1) Count-Min Sketch: تكرار الشركات
  // ======================================
  def countCompaniesCMS(df: DataFrame): CountMinSketch = {
    df
      .withColumn("company", explode(col("companies.company_name")))
      .stat
      .countMinSketch(
        col("company"),
        eps = 0.001,
        confidence = 0.99,
        seed = 1
      )
  }

  // ======================================
  // 2) Count-Min Sketch: Trending Skills
  // ======================================
  //input the date and skill output skill and frecuancy
  def trendingSkills(df: DataFrame): DataFrame = {
    df
      .withColumn("skill", explode(col("skills_after")))
      .groupBy("date", "skill")
      .count()
      .orderBy(col("date"), desc("count"))
  }


  // ======================================
  // 3) تشابه الأشخاص بالمهارات (Jaccard)
  // ======================================
  //input name of
  def similarPeopleBySkills(df: DataFrame): DataFrame = {
    val a = df.select(col("id").as("id_a"), col("skills_after").as("skills_a"))
    val b = df.select(col("id").as("id_b"), col("skills_after").as("skills_b"))

    a.crossJoin(b)
      .filter(col("id_a") < col("id_b"))
      .withColumn(
        "jaccard",
        size(array_intersect(col("skills_a"), col("skills_b"))) /
          size(array_union(col("skills_a"), col("skills_b")))
      )
      .filter(col("jaccard") >= 0.5)
      .select("id_a", "id_b", "jaccard")
  }

  // ======================================
  // 4) تشابه المسارات المهنية (Jaccard)
  // ======================================
  def similarCareerPaths(df: DataFrame): DataFrame = {
    val paths = df
      .withColumn("company", explode(col("companies.company_name")))
      .groupBy("id")
      .agg(collect_set("company").as("companies"))

    val a = paths.select(col("id").as("id_a"), col("companies").as("c_a"))
    val b = paths.select(col("id").as("id_b"), col("companies").as("c_b"))

    a.crossJoin(b)
      .filter(col("id_a") < col("id_b"))
      .withColumn(
        "jaccard",
        size(array_intersect(col("c_a"), col("c_b"))) /
          size(array_union(col("c_a"), col("c_b")))
      )
      .filter(col("jaccard") >= 0.5)
      .select("id_a", "id_b", "jaccard")
  }

}