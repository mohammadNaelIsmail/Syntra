package processing

import org.apache.spark.ml.feature.{MinHashLSH , HashingTF}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import scala.io.StdIn




object localitySensitiveHashing {

  //similar by skills
//  val result =
//    localitySensitiveHashing.similarToOnePersonLSHBySkiils(df, 0.5)
//
//  result.show(5,false)

  def similarToOnePersonLSHBySkiils(
                             df: DataFrame,
                             threshold: Double = 0.5
                           ): DataFrame = {
    print("Enter person ID: ")
     val    targetPersonId = StdIn.readLine()

    val windowSpec = Window
      .partitionBy("person_id")
      .orderBy(col("date").desc)

    val latestDF = df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")
      .filter(col("skills_after").isNotNull)

    val skillsDF = latestDF
      .select("person_id", "skills_after")
      .withColumn(
        "features",
        expr("transform(skills_after, x -> hash(x))")
      )
      .withColumn(
        "features",
        array_distinct(col("features"))
      )
      .withColumn(
        "features",
        udf((xs: Seq[Int]) => Vectors.dense(xs.map(_.toDouble).toArray))
          .apply(col("features"))
      )

    val lsh = new MinHashLSH()
      .setInputCol("features")
      .setOutputCol("hashes")
      .setNumHashTables(5)

    val model = lsh.fit(skillsDF)

    val targetVector = skillsDF
      .filter(col("person_id") === targetPersonId)


    model
      .approxSimilarityJoin(
        targetVector,
        skillsDF,
        1.0 - threshold,   // distance = 1 - similarity
        "distance"
      )
      .filter(col("datasetB.person_id") =!= targetPersonId)
      .select(
        col("datasetA.person_id").as("id_target"),
        col("datasetB.person_id").as("id_other"),
        (lit(1.0) - col("distance")).as("approx_jaccard")
      )
      .orderBy(desc("approx_jaccard"))
  }



  def similarCareerPathsLSH_Local(
                                   df: DataFrame,
                                 ): DataFrame = {

    val spark = df.sparkSession

    print("Enter person ID: ")
    val    targetPersonId = StdIn.readLine()

    val windowSpec = Window
      .partitionBy("person_id")
      .orderBy(col("date").desc)

    val latestDF = df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")

    val companiesDF = latestDF
      .withColumn("company", explode(col("companies.company_name")))
      .groupBy("person_id")
      .agg(collect_set("company").as("companies"))

    val hashingTF = new HashingTF()
      .setInputCol("companies")
      .setOutputCol("features")
      .setNumFeatures(1000)

    val featurizedDF = hashingTF.transform(companiesDF)


    val minHashLSH = new MinHashLSH()
      .setInputCol("features")
      .setOutputCol("hashes")
      .setNumHashTables(5)

    val lshModel = minHashLSH.fit(featurizedDF)


    val targetDF = featurizedDF
      .filter(col("person_id") === targetPersonId)

    val othersDF = featurizedDF
      .filter(col("person_id") =!= targetPersonId)


    val similarDF = lshModel
      .approxSimilarityJoin(
        targetDF,
        othersDF,
        1.0,                     // max Jaccard distance
        "jaccard_distance"
      )
      .select(
        col("datasetB.person_id").as("other_person_id"),
        (lit(1.0) - col("jaccard_distance")).as("similarity")
      )
      .orderBy(desc("similarity"))

    similarDF
  }




}
