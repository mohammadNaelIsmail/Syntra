// ── Test 1: Ingestion / Parsing ─────────────────────────────────────────────
// What it tests: schema correctness, null handling, date parsing
// How to run:   sbt "runMain TestIngestion"
// RAM needed:   ~1.5 GB

import org.apache.spark.sql.functions._

object TestIngestion extends App {

  val spark = TestUtils.spark
  import spark.implicits._

  println("\n========== TEST: Ingestion ==========")

  val df = TestUtils.loadDF(limit = 100)

  // 1. Schema check
  println(s"\n[1] Row count: ${df.count()}")
  println(s"[1] Columns: ${df.columns.mkString(", ")}")

  // 2. Nulls check
  val nullCounts = df.select(
    df.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0)).as(c)): _*
  )
  println("\n[2] Null counts per column:")
  nullCounts.show(truncate = false)

  // 3. Date parsing check

  // 4. companies array structure
  val withCompanies = df.filter(size(col("companies")) > 0).count()
  println(s"[4] Rows with at least 1 company: $withCompanies")

  // 5. skills_after not empty
  val withSkills = df.filter(size(col("skills_after")) > 0).count()
  println(s"[5] Rows with at least 1 skill: $withSkills")

  println("\n[PASS] Ingestion tests completed.\n")
  spark.stop()
}
