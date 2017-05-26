name := "scala_analysis"

version := "1.0"

scalaVersion := "2.10.4"
val sparkVersion="1.6.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.0.3"
)