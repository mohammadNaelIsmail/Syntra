// ── Test 3: Top-K (Best person by skills / companies) ───────────────────────
// What it tests: returns exactly 1 row, person_id not null, count > 0
// How to run:   sbt "runMain TestTopK"
// RAM needed:   ~1 GB

import processing.Best_Kperson_skills
import processing.Best_Kperson_company
import org.apache.spark.sql.functions._

object TestTopK extends App {

  val spark = TestUtils.spark
  println("\n========== TEST: Top-K ==========")

  val df = TestUtils.loadDF(limit = 200)

  // ── Best by skills ──────────────────────────────────────────────────────────
  println("\n[1] Best person by skills count:")
  val bestBySkills = Best_Kperson_skills.getBestPersonInMonthBySkills(df)
  bestBySkills.select("person_id", "skills_count").show(truncate = false)

  val skillsRows = bestBySkills.count()
  assert(skillsRows == 1, s"Expected 1 row, got $skillsRows")

  val skillsId = bestBySkills.select("person_id").collect()(0).getString(0)
  assert(skillsId != null, "person_id is null!")
  println(s"[1] Winner: $skillsId")

  // ── Best by companies ───────────────────────────────────────────────────────
  println("\n[2] Best person by distinct companies:")
  val bestByComp = Best_Kperson_company.getBestPersonInMonthByCompanies(df)
  bestByComp.select("person_id", "companies_count").show(truncate = false)

  val compRows = bestByComp.count()
  assert(compRows == 1, s"Expected 1 row, got $compRows")

  val compId = bestByComp.select("person_id").collect()(0).getString(0)
  assert(compId != null, "person_id is null!")
  println(s"[2] Winner: $compId")

  println("\n[PASS] Top-K tests completed.\n")
  spark.stop()
}
