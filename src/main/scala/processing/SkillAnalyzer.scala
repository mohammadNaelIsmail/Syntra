package processing

import org.apache.spark.sql.DataFrame

object SkillAnalyzer {
  def analyze(df: DataFrame): DataFrame = {
    // TODO: implement Count-Min Sketch / TF-IDF analysis
    df
  }
}
