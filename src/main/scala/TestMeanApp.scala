//-----testmean----------------
import org.apache.spark.sql.SparkSession
import analytics.MonthlyAnalyticsJob
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
object TestMeanApp  {

  def main(args: Array[String]): Unit = {

    Configurator.setRootLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Companies-LSH-and-CMS-Batch")
      .master("local[*]")
      .getOrCreate()

    // قراءة الداتا
    val df = spark.read
      .option("multiline", "true")
      .json(
        "C:\\Users\\Sanasys\\Desktop\\LinkHarvester\\src\\main\\resources\\people_1000_development.json"
      )

    // أخذ أول 1000 شخص
    val df1000 = df
      .orderBy("person_id")
      .limit(1000)

    val str=MonthlyAnalyticsJob.colectoor(df1000)
    print(str)

    spark.stop()
  }
}