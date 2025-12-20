package analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.Level
import processing.Best_Kperson_company.getBestPersonInMonthByCompanies
import processing.Best_Kperson_skills.getBestPersonInMonthBySkills
import processing.countMinSkitch.trendingSkillsCMS
import processing.countMinSkitch.countCompaniesCMSAsDF
import processing.localitySensitiveHashing.SimilarPersonByCompanies
import processing.localitySensitiveHashing.SimilarPersonBySkills
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object MonthlyAnalyticsJob {

  def colectoor(df:DataFrame): String = {
    Configurator.setRootLevel(Level.OFF)

    // ===== month =====
    val month: String =
      df.select(date_format(col("last_update"), "yyyy-MM"))
        .limit(1)
        .collect()(0)
        .getString(0)

    // ===== companies_people_count =====
    val companiesPeopleCount: List[Map[String, Any]] =
      countCompaniesCMSAsDF(df)
        .select("company", "estimated_count")
        .distinct()
        .orderBy(desc("estimated_count"))
        .limit(10)
        .collect()
        .map { r =>
          Map(
            "company" -> r.getString(0),
            "count" -> r.getLong(1)
          )
        }
        .toList


    // ===== skills_people_count =====
    val skillsPeopleCount: List[Map[String, Any]] =
      trendingSkillsCMS(df)
        .select("skill", "estimated_count")
        .distinct()
        .orderBy(desc("estimated_count"))
        .limit(20)
        .collect()
        .map { r =>
          Map(
            "skill" -> r.getString(0),
            "count" -> r.getLong(1)
          )
        }
        .toList


    // ===== similarity_by_company =====
    val similarityByCompany: List[Map[String, String]] =
      SimilarPersonByCompanies(df)
        .collect()
        .map { r =>
          Map(
            "person" -> r.getString(0),
            "most_similar_person" -> r.getString(1)
          )
        }
        .toList

    // ===== similarity_by_skills =====
    val similarityBySkills: List[Map[String, String]] =
      SimilarPersonBySkills(df)
        .collect()
        .map { r =>
          Map(
            "person" -> r.getString(0),
            "most_similar_person" -> r.getString(1)
          )
        }
        .toList

    // ===== best person =====
    val bestBySkills: String =
      getBestPersonInMonthBySkills(df)
        .select("person_id")
        .limit(1)
        .collect()
        .headOption
        .map(_.getString(0))
        .orNull

    val bestByCompany: String =
      getBestPersonInMonthByCompanies(df)
        .select("person_id")
        .limit(1)
        .collect()
        .headOption
        .map(_.getString(0))
        .orNull

    // ===== final JSON object =====
    val jsonObject: Map[String, Any] = Map(
      "month" -> month,
      "skills_people_count" -> skillsPeopleCount,
      "companies_people_count" -> companiesPeopleCount,
      "similarity_by_company" -> similarityByCompany,
      "similarity_by_skills" -> similarityBySkills,
      "best_person_in_month_by_skills" -> bestBySkills,
      "best_person_in_month_by_company" -> bestByCompany
    )

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(jsonObject)
  }
}


//{ "month": "2023-01",
//  "skills_people_count": [ { "skill": "Spark", "count": 341 },{ "skill": "Python", "count": 321 }, { "skill": "Docker", "count": 308 } ],
//  "companies_people_count": [ { "company": "Amazon", "count": 9813 }, { "company": "Google", "count": 9633 }, { "company": "IBM", "count": 7451 } ],
//  "similarity_by_company": [ { "person": "person_0", "most_similar_person": "person_42" }, { "person": "person_1", "most_similar_person": "person_87" } ],
//  "similarity_by_skills": [ { "person": "person_0", "most_similar_person": "person_19" }, { "person": "person_1", "most_similar_person": "person_55" } ],
//  "best_person_in_month_by_skills": "person_228",
//  "best_person_in_month_by_company": "person_102"
//}