ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "LinkHarvester",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-mllib"          % "3.5.0",
      "org.elasticsearch" %% "elasticsearch-spark-30" % "8.19.0",
      "io.spray" %% "spray-json" % "1.3.6"
    )
  )
