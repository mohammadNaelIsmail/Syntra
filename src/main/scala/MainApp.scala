import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import storage.ElasticsearchWriter

object MainApp {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MainApp")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.json("src/main/resources/people_1000_development.json")

    ElasticsearchWriter.write(df, "people")
  }
}
