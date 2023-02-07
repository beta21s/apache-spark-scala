ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "spark-join",
    idePackagePrefix := Some("com.truongtpa")
  )

val sparkVersion = "3.0.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,

)

// libraryDependencies += "org.apache.ignite" % "ignite-spark-ext" % "3.0.0"
libraryDependencies += "com.redislabs" %% "spark-redis" % "3.0.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.4"
libraryDependencies += "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}