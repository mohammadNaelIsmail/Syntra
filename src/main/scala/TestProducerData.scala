// ── Test 6: Producer data validation (Python-free) ──────────────────────────
// What it tests: the JSON file the producer reads is well-formed
// How to run:   sbt "runMain TestProducerData"
// RAM needed:   ~1 GB

object TestProducerData extends App {

  val spark = TestUtils.spark
  println("\n========== TEST: Producer Data Validation ==========")

  // Load full 1000-record file
  val df = TestUtils.loadDF(limit = 1000)
  val total = df.count()
  println(s"\n[1] Total records loaded: $total")
  assert(total > 0, "No records loaded — check DATA_PATH in TestUtils")

  import org.apache.spark.sql.functions._

  // person_id uniqueness
  val distinct = df.select("person_id").distinct().count()
  println(s"[2] Distinct person_ids: $distinct / $total")

  // null person_ids
  val nullIds = df.filter(col("person_id").isNull).count()
  assert(nullIds == 0, s"$nullIds rows have null person_id!")
  println(s"[3] Null person_ids: $nullIds")

  // date range sanity
  println("\n[4] Date range:")
  df.agg(
    min("last_update").as("earliest"),
    max("last_update").as("latest")
  ).show(truncate = false)

  // companies array: check no empty arrays where there should be data
  val emptyCompanies = df.filter(size(col("companies")) === 0).count()
  println(s"[5] Rows with 0 companies: $emptyCompanies / $total")

  println("\n[PASS] Producer data validation completed.\n")
  spark.stop()
}
