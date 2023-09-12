ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"
val sparkVersion = "3.4.1"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

lazy val root = (project in file("."))
  .settings(
    name := "ProjectClientsBillsForVS"
  )
