ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "us-accidents-warehouse",
    idePackagePrefix := Some("pl.michalsz.spark")
    )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided",
  "io.delta" %% "delta-core" % "1.0.0",
  "com.swoop" %% "spark-alchemy" % "1.1.0"
  )

