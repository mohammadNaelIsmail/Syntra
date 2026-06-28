import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// ── Shared test fixture ─────────────────────────────────────────────────────
// Every test object calls TestUtils.spark and TestUtils.loadDF
// No Kafka. No Elasticsearch. Pure local Spark on the JSON file.

object TestUtils {

  val DATA_PATH = "src/main/resources/people_1000_development.json"

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("LinkHarvester-Tests")
    .master("local[2]")                  // 2 cores max to save RAM
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")  // default 200 kills RAM
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  val schema = new StructType()
    .add("person_id",    StringType)
    .add("name",         StringType)
    .add("skills_before",ArrayType(StringType))
    .add("skills_after", ArrayType(StringType))
    .add("new_skill",    StringType)
    .add("date",         StringType)
    .add("profile_text", StringType)
    .add("current_status", StringType)
    .add("companies", ArrayType(
      new StructType()
        .add("company_name", StringType)
        .add("date_from",    StringType)
        .add("date_to",      StringType)
    ))

  // Load JSON directly — no Kafka involved
  def loadDF(limit: Int = 200): DataFrame =
    spark.read.option("multiline", "true")
      
      .json(DATA_PATH)
      .limit(limit)   // keep it small for low-RAM machines

}
