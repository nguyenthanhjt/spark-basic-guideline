ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "cncf",
    idePackagePrefix := Some("com.trinity")
  )
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.3.4",
  "org.apache.hadoop" % "hadoop-client" % "3.3.1" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs-client" % "3.3.4",
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2" % "provided"
)