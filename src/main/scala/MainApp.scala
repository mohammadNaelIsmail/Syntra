import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import scala.io.StdIn
import storage.OpenSearchWriter
import processing.Best_Kperson_skills
import processing.Best_Kperson_company
import processing.localitySensitiveHashing

object MainApp {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MainApp")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.option("multiline", "true").json("src/main/resources/people_1000_development.json")
    val resultDF = localitySensitiveHashing.similarCareerPathsLSH_Local(df)
    resultDF.show(10, truncate = false)






    spark.stop()
    //OpenSearchWriter.write(df, "people")
  }
}
