ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "LinkHarvester",
    libraryDependencies ++= Seq(
      "org.apache.spark"         %% "spark-sql-kafka-0-10"   % "3.5.0",
      "org.apache.spark"         %% "spark-sql"              % "3.5.0",
      "org.apache.spark"         %% "spark-mllib"            % "3.5.0",
      "org.elasticsearch"        %% "elasticsearch-spark-30" % "8.19.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
      "com.typesafe"              % "config"                  % "1.4.3"
    )
  )

fork in run := true
run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED"

)

run / javaOptions += "-Dlog4j.rootCategory=ERROR,console"
