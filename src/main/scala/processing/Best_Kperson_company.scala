package processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Best_Kperson_company {

private def getLatestPersonState(df: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy("person_id")
      .orderBy(col("last_update").desc)

    df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")
  }

  def getBestPersonInMonthByCompanies(df: DataFrame): DataFrame = {

    getLatestPersonState(df)
      .withColumn(
        "company_names",
        expr("transform(companies, c -> c.company_name)")
      )
      .withColumn(
        "companies_count",
        when(col("company_names").isNull, 0)
          .otherwise(size(array_distinct(col("company_names"))))
      )
      .orderBy(desc("companies_count"))
      .limit(1)
  }
}

//استدعاء

//val top10ByCompanies =
//  Best_Kperson_company.getTopKByCompanies(df, 10)
//
//top10ByCompanies.show(false)
