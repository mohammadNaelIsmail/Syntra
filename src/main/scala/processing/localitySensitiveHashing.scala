package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors

object localitySensitiveHashing {



  def SimilarPersonBySkills(df: DataFrame): DataFrame = {


    val windowSpec = Window
      .partitionBy("person_id")
      .orderBy(col("last_update").desc)

    val latestDF = df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")
      .filter(col("skills_after").isNotNull)


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


    val lsh = new MinHashLSH()
      .setInputCol("features")
      .setOutputCol("hashes")
      .setNumHashTables(5)

    val model = lsh.fit(skillsDF)


    val simDF = model
      .approxSimilarityJoin(
        skillsDF,
        skillsDF,
        1.0,
        "distance"
      )
      .filter(col("datasetA.person_id") =!= col("datasetB.person_id"))
      .withColumn("approx_jaccard", lit(1.0) - col("distance"))


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



    def SimilarPersonByCompanies(df: DataFrame): DataFrame = {

      val windowSpec = Window
        .partitionBy("person_id")
        .orderBy(col("last_update").desc)

      val latestDF = df
        .withColumn("rn", row_number().over(windowSpec))
        .filter(col("rn") === 1)
        .drop("rn")
        .filter(col("companies").isNotNull)

            val companiesDF = latestDF
        .select(
          col("person_id"),
          expr("transform(companies, c -> c.company_name)").as("company_names")
        )
        .filter(size(col("company_names")) > 0)


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


      val lsh = new MinHashLSH()
        .setInputCol("features")
        .setOutputCol("hashes")
        .setNumHashTables(5)

      val model = lsh.fit(featuresDF)


      val simDF = model
        .approxSimilarityJoin(
          featuresDF,
          featuresDF,
          1.0,
          "distance"
        )
        .filter(col("datasetA.person_id") =!= col("datasetB.person_id"))
        .withColumn("similarity", lit(1.0) - col("distance"))


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
