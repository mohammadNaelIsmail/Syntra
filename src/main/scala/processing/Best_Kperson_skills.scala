package processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Best_Kperson_skills {

  // 1️⃣ آخر تحديث لكل شخص + عدد المهارات
  def getLatestPersonStateWithSkillsCount(df: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy("person_id")
      .orderBy(col("date").desc)

    df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")
      .withColumn(
        "skills_count",
        when(col("skills_after").isNull, 0)
          .otherwise(size(col("skills_after")))
      )
  }

  // 2️⃣ ترتيب الجميع وأخذ أعلى K
  def getTopKBySkills(df: DataFrame, k: Int): DataFrame = {

    getLatestPersonStateWithSkillsCount(df)
      .orderBy(desc("skills_count"))   // ✅ ترتيب كل الأشخاص
      .limit(k)                        // ✅ أخذ أول K
  }
}
//الاستدعاء
//
//val top10 =
//  Best_Kperson_skills.getTopKBySkills(df, 100)
//
//top10
//  .select("person_id", "name", "skills_count")
//  .show(100, false)