import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.CountMinSketch
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.{MinHashLSH}
import org.apache.spark.ml.linalg.Vectors
import scala.io.StdIn


object countMinSkitch {

  // ======================================
  // 1)عدد الاشخاص في كل شركة Google
  // ======================================
  //الاستدعاء
//  val cms = countMinSkitch.countCompaniesCMS(df)
//  print("Enter company name: ")
//  val companyName = StdIn.readLine()
//  println("Estimated count for " + companyName + " = " + cms.estimateCount(companyName))

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
  //الاستدعارء في المين
  //    val trendingDF = countMinSkitch.trendingSkills(df)
  //    trendingDF.show(10, truncate = false)
  //    val topRow = trendingDF.first()
  //    val skillName = topRow.getAs[String]("skill")
  //    val count = topRow.getAs[Long]("count")
  //    println(s"""The most trending skill is :- $skillName \nand the number of people who learned it is :- $count""")

  def trendingSkills(df: DataFrame): DataFrame = {
    df
      .withColumn("skill", explode(col("skills_after")))
      .groupBy("date", "skill")
      .count()
      .orderBy(col("date"), desc("count"))
  }





  // ======================================
  // 4) تشابه المسارات المهنية (Jaccard)
  // ======================================
  def similarCareerPaths(df: DataFrame): DataFrame = {

    val paths = df
      .withColumn("company", explode(col("companies.company_name")))
      .groupBy("person_id")
      .agg(collect_set("company").as("companies"))

    val a = paths.select(
      col("person_id").as("id_a"),
      col("companies").as("c_a")
    )

    val b = paths.select(
      col("person_id").as("id_b"),
      col("companies").as("c_b")
    )


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