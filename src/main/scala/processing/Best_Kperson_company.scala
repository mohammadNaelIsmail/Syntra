package processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Best_Kperson_company {


  // 🔹 آخر تحديث لكل شخص
  private def getLatestPersonState(df: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy("person_id")
      .orderBy(col("date").desc)

    df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")
  }

  // 🔹 Top K حسب عدد الشركات
  def getTopKByCompanies(df: DataFrame, k: Int): DataFrame = {

    getLatestPersonState(df)
      // استخراج أسماء الشركات فقط
      .withColumn(
        "company_names",
        expr("transform(companies, c -> c.company_name)")
      )
      // حساب عدد الشركات (distinct)
      .withColumn(
        "companies_count",
        when(col("company_names").isNull, 0)
          .otherwise(size(array_distinct(col("company_names"))))
      )
      .orderBy(desc("companies_count")) // ترتيب الجميع
      .limit(k)                          // أخذ Top K
      .select(
        col("person_id"),
        col("name"),
        col("companies_count"),
        col("company_names")
      )
  }
}

//استدعاء

//val top10ByCompanies =
//  Best_Kperson_company.getTopKByCompanies(df, 10)
//
//top10ByCompanies.show(false)
