
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import ingestion.ProcessDataReader

object MainForMonthDataProcess {
  def main(args: Array[String]): Unit = {

    Configurator.setRootLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("MainApp-Monthly-Kafka")
      .master("local[*]")
      .getOrCreate()

    ProcessDataReader.processDataReader(
      spark = spark,
      bootstrapServers = "localhost:9092",
      topicPrefix = "people"
    )

  }
}
