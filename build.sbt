ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "LinkHarvester",
    resolvers += "OpenSearch Maven" at "https://artifacts.opensearch.org/maven/",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.opensearch.client" %% "opensearch-spark-30" % "1.3.0"
    )
  )
