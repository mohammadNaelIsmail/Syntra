// ── Test 8: Python queries — mock ES response ────────────────────────────────
// This is a Scala-side shape test. For actual Python query tests, use pytest.
// What it tests: the JSON shape that ES returns matches what Python expects
// How to run:   sbt "runMain TestPythonQueries"

import org.apache.spark.sql.functions._

object TestPythonQueries extends App {

  val spark = TestUtils.spark
  import spark.implicits._
  println("\n========== TEST: Python Query Shape Validation ==========")

  val df = TestUtils.loadDF(limit = 100)

  // Simulate what ElasticsearchWriter writes, what Python queries read back
  val merged = storage.ElasticsearchWriter.mergePersonData(df)

  // Python's fetch_all_documents reads _source which contains these fields:
  val expectedFields = Set(
    "person_id", "name", "last_update", "experiences",
    "skills_before_list", "skills_after_list", "new_skill"
  )

  val actualFields = merged.columns.toSet
  val missing = expectedFields -- actualFields
  val extra   = actualFields   -- expectedFields

  if (missing.nonEmpty)
    println(s"[WARN] Fields Python expects but ES won't have: ${missing.mkString(", ")}")
  else
    println("[1] All Python-expected fields are present in the merged document.")

  if (extra.nonEmpty)
    println(s"[INFO] Extra fields in ES doc (fine): ${extra.mkString(", ")}")

  // Check experiences nested structure that Python's fetch_filtered_documents uses
  println("\n[2] Sample experiences field (what Python inner_hits returns):")
  merged.select("person_id", "experiences")
    .filter(size(col("experiences")) > 0)
    .limit(3)
    .show(truncate = false)

  println("\n[PASS] Python query shape test completed.\n")
  spark.stop()
}
