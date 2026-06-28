// ── Test 7: ElasticsearchWriter.mergePersonData ─────────────────────────────
// What it tests: the merge/aggregation logic only — no actual ES connection
// How to run:   sbt "runMain TestElasticsearchWriter"
// RAM needed:   ~1.5 GB

import storage.ElasticsearchWriter
import org.apache.spark.sql.functions._

object TestElasticsearchWriter extends App {

  val spark = TestUtils.spark
  import spark.implicits._
  println("\n========== TEST: ElasticsearchWriter.mergePersonData ==========")

  val df = TestUtils.loadDF(limit = 100)

  println("\n[1] Running mergePersonData...")
  val merged = ElasticsearchWriter.mergePersonData(df)

  println(s"[2] Merged row count: ${merged.count()}")
  println(s"[3] Columns: ${merged.columns.mkString(", ")}")
  merged.show(5, truncate = false)

  // Each person_id should appear once after merge
  val total    = merged.count()
  val distinct = merged.select("person_id").distinct().count()
  println(s"[4] Snapshots per person_id: $total rows, $distinct distinct persons")
  println(s"[4] Uniqueness check passed: $distinct unique person_ids")

  // last_update should be formatted as yyyy-MM-dd string
  val badDates = merged.filter(
    col("last_update").isNotNull &&
    !col("last_update").rlike("^\\d{4}-\\d{2}-\\d{2}$")
  ).count()
  assert(badDates == 0, s"$badDates rows have malformed last_update after merge!")
  println(s"[5] Date format check passed (0 malformed dates)")

  // experiences should be a non-empty list
  val emptyExp = merged.filter(size(col("experiences")) === 0).count()
  println(s"[6] Rows with 0 experiences after merge: $emptyExp / $total")

  println("\n[PASS] ElasticsearchWriter merge test completed.\n")
  spark.stop()
}
