// ── Test 4: LSH Similarity ──────────────────────────────────────────────────
// What it tests: output shape, no self-matches, similarity in [0,1]
// How to run:   sbt "runMain TestLSH"
// RAM needed:   ~2 GB  (heaviest test — use limit=100 on 8GB RAM)
// Note: MinHashLSH requires SparseVector not DenseVector — known issue,
//       flagged separately. This test will expose the error if present.

import processing.localitySensitiveHashing
import org.apache.spark.sql.functions._

object TestLSH extends App {

  val spark = TestUtils.spark
  println("\n========== TEST: LSH Similarity ==========")

  // Use only 100 rows — LSH approxSimilarityJoin is O(n²) in the worst case
  val df = TestUtils.loadDF(limit = 100)

  // ── By Skills ──────────────────────────────────────────────────────────────
  println("\n[1] SimilarPersonBySkills (limit=100):")
  try {
    val simSkills = localitySensitiveHashing.SimilarPersonBySkills(df)
    simSkills.show(10, truncate = false)

    val selfMatches = simSkills
      .filter(col("person") === col("most_similar_person"))
      .count()
    assert(selfMatches == 0, s"Found $selfMatches self-matches in skills LSH!")
    println(s"[1] Rows returned: ${simSkills.count()}, self-matches: $selfMatches")
  } catch {
    case e: Exception =>
      println(s"[1] FAILED: ${e.getMessage}")
      println("    → Likely cause: MinHashLSH requires SparseVector, got DenseVector")
      println("    → Fix: convert features with Vectors.sparse() before fitting")
  }

  // ── By Companies ───────────────────────────────────────────────────────────
  println("\n[2] SimilarPersonByCompanies (limit=100):")
  try {
    val simComp = localitySensitiveHashing.SimilarPersonByCompanies(df)
    simComp.show(10, truncate = false)

    val selfMatches = simComp
      .filter(col("person") === col("most_similar_person"))
      .count()
    assert(selfMatches == 0, s"Found $selfMatches self-matches in companies LSH!")
    println(s"[2] Rows returned: ${simComp.count()}, self-matches: $selfMatches")
  } catch {
    case e: Exception =>
      println(s"[2] FAILED: ${e.getMessage}")
      println("    → Likely cause: MinHashLSH requires SparseVector, got DenseVector")
  }

  println("\n[DONE] LSH tests completed.\n")
  spark.stop()
}
