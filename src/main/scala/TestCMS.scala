// ── Test 2: Count-Min Sketch (skills + companies) ───────────────────────────
// What it tests: CMS output shape, count sanity, no nulls in results
// How to run:   sbt "runMain TestCMS"
// RAM needed:   ~1.5 GB

import processing.countMinSkitch
import org.apache.spark.sql.functions._

object TestCMS extends App {

  val spark = TestUtils.spark
  println("\n========== TEST: Count-Min Sketch ==========")

  val df = TestUtils.loadDF(limit = 200)

  // ── Skills CMS ─────────────────────────────────────────────────────────────
  println("\n[1] trendingSkillsCMS:")
  val skillsCMS = countMinSkitch.trendingSkillsCMS(df)
  skillsCMS.orderBy(desc("estimated_count")).show(10, truncate = false)

  val skillNulls = skillsCMS.filter(col("estimated_count").isNull).count()
  assert(skillNulls == 0, "Null estimated_count in skills CMS!")

  val zeroSkills = skillsCMS.filter(col("estimated_count") === 0).count()
  println(s"[1] Skills with estimated_count=0 (should be 0): $zeroSkills")

  // ── Companies CMS ──────────────────────────────────────────────────────────
  println("\n[2] countCompaniesCMSAsDF:")
  val companiesCMS = countMinSkitch.countCompaniesCMSAsDF(df)
  companiesCMS.orderBy(desc("estimated_count")).show(10, truncate = false)

  val compNulls = companiesCMS.filter(col("estimated_count").isNull).count()
  assert(compNulls == 0, "Null estimated_count in companies CMS!")

  // ── Sanity: compare CMS vs exact count ─────────────────────────────────────
  println("\n[3] CMS vs exact count comparison (top 5 skills):")
  val exactSkills = df
    .withColumn("skill", explode(col("skills_after")))
    .groupBy("skill").count()
    .orderBy(desc("count"))
    .limit(5)

  val cmsTop5 = skillsCMS
    .orderBy(desc("estimated_count"))
    .limit(5)
    .select("skill", "estimated_count")

  println("Exact:")
  exactSkills.show(truncate = false)
  println("CMS estimated:")
  cmsTop5.show(truncate = false)

  println("\n[PASS] CMS tests completed.\n")
  spark.stop()
}
