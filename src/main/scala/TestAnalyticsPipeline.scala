// ── Test 5: Full Analytics Pipeline (no Kafka, no ES) ───────────────────────
// What it tests: MonthlyAnalyticsJob.colectoor() produces valid JSON
// How to run:   sbt "runMain TestAnalyticsPipeline"
// RAM needed:   ~2 GB

import analytics.MonthlyAnalyticsJob
import com.fasterxml.jackson.databind.ObjectMapper

object TestAnalyticsPipeline extends App {

  val spark = TestUtils.spark
  println("\n========== TEST: Full Analytics Pipeline ==========")

  val df = TestUtils.loadDF(limit = 150)

  println("\n[1] Running MonthlyAnalyticsJob.colectoor()...")
  val json = MonthlyAnalyticsJob.colectoor(df)

  println("\n[2] Raw output:")
  println(json)

  // ── Validate JSON structure ─────────────────────────────────────────────────
  println("\n[3] Validating JSON structure...")
  val mapper = new ObjectMapper()
  val node   = mapper.readTree(json)  // throws if invalid JSON

  val requiredKeys = List(
    "month",
    "skills_people_count",
    "companies_people_count",
    "similarity_by_company",
    "similarity_by_skills",
    "best_person_in_month_by_skills",
    "best_person_in_month_by_company"
  )

  requiredKeys.foreach { key =>
    assert(node.has(key), s"Missing key in output JSON: $key")
    println(s"  ✓ $key present")
  }

  // ── Value checks ───────────────────────────────────────────────────────────
  assert(node.get("month").asText().matches("\\d{4}-\\d{2}"),
    "month format should be yyyy-MM")

  assert(node.get("skills_people_count").isArray,
    "skills_people_count should be array")

  assert(node.get("best_person_in_month_by_skills").asText().nonEmpty,
    "best_person_in_month_by_skills should not be empty")

  println("\n[PASS] Analytics pipeline test completed.\n")
  spark.stop()
}
