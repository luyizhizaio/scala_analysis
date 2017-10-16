name := "scala_analysis"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion="1.6.2"




libraryDependencies  ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.avro" % "avro" % "1.7.7",
  "mysql" % "mysql-connector-java" % "5.1.34",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "com.twitter" %% "chill-avro" % "0.6.0",
  "com.databricks" %% "spark-csv" % "1.0.3",
  "org.scalanlp" % "epic_2.10" % "0.3.1",
  "org.scalanlp" % "epic-parser-en-span_2.10" % "2015.2.19",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1" classifier "models",
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",
  "org.scalanlp" %% "breeze-viz" % "0.12"
)

