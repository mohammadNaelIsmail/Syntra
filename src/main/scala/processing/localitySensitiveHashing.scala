package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.{Vectors, SparseVector}

object localitySensitiveHashing {

  // بناء sparse binary vector من قائمة عناصر مقابل vocabulary
  private def buildSparseVector(items: Seq[String], vocab: Map[String, Int]): SparseVector = {
    val indices = items.flatMap(vocab.get).distinct.sorted.toArray
    val values  = Array.fill(indices.length)(1.0)
    Vectors.sparse(vocab.size, indices, values).toSparse
  }

  def SimilarPersonBySkills(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val windowSpec = Window.partitionBy("person_id").orderBy(col("last_update").desc)

    val latestDF = df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1).drop("rn")
      .filter(col("skills_after").isNotNull)
      .select("person_id", "skills_after")

    // بناء vocabulary من كل المهارات الموجودة
    val allSkills = latestDF
      .withColumn("skill", explode(col("skills_after")))
      .select("skill").distinct()
      .collect().map(_.getString(0))
    val vocab = allSkills.zipWithIndex.toMap

    val buildVec = udf((skills: Seq[String]) => buildSparseVector(skills, vocab))

    val featuresDF = latestDF
      .withColumn("features", buildVec(col("skills_after")))
      .filter(col("features").isNotNull)

    val lsh   = new MinHashLSH().setInputCol("features").setOutputCol("hashes").setNumHashTables(5)
    val model = lsh.fit(featuresDF)

    val simDF = model
      .approxSimilarityJoin(featuresDF, featuresDF, 1.0, "distance")
      .filter(col("datasetA.person_id") =!= col("datasetB.person_id"))
      .withColumn("approx_jaccard", lit(1.0) - col("distance"))

    val rankWindow = Window.partitionBy(col("datasetA.person_id")).orderBy(col("approx_jaccard").desc)

    simDF
      .withColumn("rn", row_number().over(rankWindow))
      .filter(col("rn") === 1)
      .select(
        col("datasetA.person_id").as("person"),
        col("datasetB.person_id").as("most_similar_person")
      )
  }

  def SimilarPersonByCompanies(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val windowSpec = Window.partitionBy("person_id").orderBy(col("last_update").desc)

    val latestDF = df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1).drop("rn")
      .filter(col("companies").isNotNull)
      .select(
        col("person_id"),
        expr("transform(companies, c -> c.company_name)").as("company_names")
      )
      .filter(size(col("company_names")) > 0)

    val allCompanies = latestDF
      .withColumn("company", explode(col("company_names")))
      .select("company").distinct()
      .collect().map(_.getString(0))
    val vocab = allCompanies.zipWithIndex.toMap

    val buildVec = udf((names: Seq[String]) => buildSparseVector(names, vocab))

    val featuresDF = latestDF
      .withColumn("features", buildVec(col("company_names")))
      .filter(col("features").isNotNull)

    val lsh   = new MinHashLSH().setInputCol("features").setOutputCol("hashes").setNumHashTables(5)
    val model = lsh.fit(featuresDF)

    val simDF = model
      .approxSimilarityJoin(featuresDF, featuresDF, 1.0, "distance")
      .filter(col("datasetA.person_id") =!= col("datasetB.person_id"))
      .withColumn("similarity", lit(1.0) - col("distance"))

    val rankWindow = Window.partitionBy(col("datasetA.person_id")).orderBy(col("similarity").desc)

    simDF
      .withColumn("rn", row_number().over(rankWindow))
      .filter(col("rn") === 1)
      .select(
        col("datasetA.person_id").as("person"),
        col("datasetB.person_id").as("most_similar_person")
      )
  }
}
