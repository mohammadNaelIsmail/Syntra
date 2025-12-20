package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors

object localitySensitiveHashing {

  // =========================
  // فنكشن: يرجع لكل شخص أكثر شخص يشابهه
  // =========================

  def SimilarPersonBySkills(df: DataFrame): DataFrame = {

    // 1) آخر حالة لكل شخص
    val windowSpec = Window
      .partitionBy("person_id")
      .orderBy(col("last_update").desc)

    val latestDF = df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")
      .filter(col("skills_after").isNotNull)

    // 2) تجهيز الميزات (skills → vector)
    val skillsDF = latestDF
      .select("person_id", "skills_after")
      .withColumn(
        "features",
        array_distinct(expr("transform(skills_after, x -> hash(x))"))
      )
      .withColumn(
        "features",
        udf((xs: Seq[Int]) =>
          Vectors.dense(xs.map(_.toDouble).toArray)
        ).apply(col("features"))
      )

    // 3) تدريب LSH
    val lsh = new MinHashLSH()
      .setInputCol("features")
      .setOutputCol("hashes")
      .setNumHashTables(5)

    val model = lsh.fit(skillsDF)

    // 4) تشابه الجميع مع الجميع
    val simDF = model
      .approxSimilarityJoin(
        skillsDF,
        skillsDF,
        1.0,          // مسافة كبيرة للسماح بكل المقارنات
        "distance"
      )
      .filter(col("datasetA.person_id") =!= col("datasetB.person_id"))
      .withColumn("approx_jaccard", lit(1.0) - col("distance"))

    // 5) اختيار أكثر شخص مشابه لكل شخص
    val rankWindow = Window
      .partitionBy(col("datasetA.person_id"))
      .orderBy(col("approx_jaccard").desc)

    simDF
      .withColumn("rn", row_number().over(rankWindow))
      .filter(col("rn") === 1)
      .select(
        col("datasetA.person_id").as("person"),
        col("datasetB.person_id").as("most_similar_person")
      )
  }



    // =====================================================
    // ترجع لكل شخص أكثر شخص يشابهه حسب الشركات (LSH)
    // =====================================================
    def SimilarPersonByCompanies(df: DataFrame): DataFrame = {

      // 1) آخر حالة لكل شخص
      val windowSpec = Window
        .partitionBy("person_id")
        .orderBy(col("last_update").desc)

      val latestDF = df
        .withColumn("rn", row_number().over(windowSpec))
        .filter(col("rn") === 1)
        .drop("rn")
        .filter(col("companies").isNotNull)

      // 2) استخراج أسماء الشركات فقط
      val companiesDF = latestDF
        .select(
          col("person_id"),
          expr("transform(companies, c -> c.company_name)").as("company_names")
        )
        .filter(size(col("company_names")) > 0)

      // 3) تجهيز الميزات (companies → vector)
      val featuresDF = companiesDF
        .withColumn(
          "features_raw",
          array_distinct(expr("transform(company_names, x -> hash(x))"))
        )
        .withColumn(
          "features",
          udf((xs: Seq[Int]) =>
            Vectors.dense(xs.map(_.toDouble).toArray)
          ).apply(col("features_raw"))
        )
        .drop("features_raw")

      // 4) تدريب MinHash LSH
      val lsh = new MinHashLSH()
        .setInputCol("features")
        .setOutputCol("hashes")
        .setNumHashTables(5)

      val model = lsh.fit(featuresDF)

      // 5) تشابه الجميع مع الجميع (LSH)
      val simDF = model
        .approxSimilarityJoin(
          featuresDF,
          featuresDF,
          1.0,
          "distance"
        )
        .filter(col("datasetA.person_id") =!= col("datasetB.person_id"))
        .withColumn("similarity", lit(1.0) - col("distance"))

      // 6) اختيار أكثر شخص مشابه لكل شخص
      val rankWindow = Window
        .partitionBy(col("datasetA.person_id"))
        .orderBy(col("similarity").desc)

      simDF
        .withColumn("rn", row_number().over(rankWindow))
        .filter(col("rn") === 1)
        .select(
          col("datasetA.person_id").as("person"),
          col("datasetB.person_id").as("most_similar_person")
        )
    }

    // =====================================================
    // MAIN
    // =====================================================



//الاستدعاء
//  على شكل df [personid|similar]
//    val resultDF = localitySensitiveHashing.SimilarPersonBySkills(df)
//    val resultDFcomp = localitySensitiveHashing.mostSimilarPersonByCompanies(df)
//    resultDF.show(false)
//    resultDFcomp.show(false)

}

//{key"skillspeoplecount,value[{skill1,val1}]
//{key"companypeoplecount,value[{company1,val1}]}
//{key"semelaritybycompany",value[{p0,p1},{p1,p2}] }
//{key"semelaritybyskills[{p0,p1},{p1,p2}]
//{key"bestpersoninmonthbyskills,value"person"}
//{key"bestpersoninmonthbycompany{value"person0"}
