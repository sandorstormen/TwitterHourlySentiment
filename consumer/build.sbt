name := "Consumer"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.spark" %% "spark-streaming" % "2.4.8",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.8",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.4.8",
  "databricks" % "spark-corenlp" % "0.4.0-spark2.4-scala2.11",
  "com.johnsnowlabs.nlp" %% "spark-nlp-spark24" % "3.3.0",
  "org.vegas-viz" %% "vegas" % "0.3.9",
  "org.vegas-viz" %% "vegas-spark" % "0.3.9"

 )
