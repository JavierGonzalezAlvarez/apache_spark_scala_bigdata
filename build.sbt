name := "spark-bigdata"

version := "0.1"

scalaVersion := "2.12.11"

idePackagePrefix := Some("org.bigdata.application")

// dependencies
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"

/*
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4"
)
*/