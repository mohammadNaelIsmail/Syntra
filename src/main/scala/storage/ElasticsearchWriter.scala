package storage

import config.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object ElasticsearchWriter {

  private val esOptions = Map(
    "es.nodes" -> AppConfig.Elasticsearch.nodes,
    "es.port" -> AppConfig.Elasticsearch.port,
    "es.nodes.wan.only" -> "true"
  )

  def mergePersonData(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val dfWithCompany = if (df.columns.contains("companies")) {
      df.withColumn("company", explode_outer($"companies"))
    } else if (df.columns.contains("company_name")) {
      df.withColumn("company", struct(
        $"company_name".as("company_name"),
        $"date_from".as("date_from"),
        $"date_to".as("date_to")
      ))
    } else {
      throw new IllegalArgumentException("DataFrame must contain 'companies' or 'company_name' column")
    }

    val normalized = dfWithCompany
      .withColumn(
        "experience_struct",
        struct(
          $"company.company_name".cast(StringType).as("company_name"),
          date_format($"company.date_from".cast(DateType), "yyyy-MM-dd").as("date_from"),
          date_format(
            coalesce($"company.date_to".cast(DateType), current_date()),
            "yyyy-MM-dd"
          ).as("date_to")
        )
      )
      .select(
        $"person_id",
        $"name",
        $"profile_text",
        $"current_status",
        $"last_update",
        $"new_skill",
        $"experience_struct",
        $"skills_before",
        $"skills_after"
      )
      .dropDuplicates(
        "person_id",
        "experience_struct",
        "new_skill",
        "last_update"
      )

    normalized
      .groupBy(
        $"person_id",
        $"name",
        $"profile_text",
        $"current_status",
        $"last_update"
      )
      .agg(
        max($"new_skill").as("new_skill"),
        collect_set($"experience_struct").as("experiences"),
        first($"skills_before", ignoreNulls = true).as("skills_before_list"),
        first($"skills_after", ignoreNulls = true).as("skills_after_list")
      )
      .withColumn("last_update", date_format($"last_update".cast(DateType), "yyyy-MM-dd"))
  }

  def writeBatch(df: DataFrame, index: String, idCol: String): Unit = {
    df.write
      .format("org.elasticsearch.spark.sql")
      .options(esOptions)
      .option("es.resource", index)
      .option("es.mapping.id", idCol)
      .option("es.write.operation", "upsert")
      .mode("append")
      .save()
  }

  def writeStream(df: DataFrame, index: String = AppConfig.Elasticsearch.snapshotIndex): StreamingQuery = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val mergedDF = mergePersonData(batchDF)
          .dropDuplicates("person_id")

        mergedDF.write
          .format("org.elasticsearch.spark.sql")
          .options(esOptions)
          .option("es.resource", index)
          .option("es.mapping.id", "person_id")
          .option("es.write.operation", "index")
          .mode("append")
          .save()

        println(s"Batch $batchId -> ES[$index]")
      }
      .option("checkpointLocation", AppConfig.Checkpoints.snapshot)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
  }
}