name := "Spark_Hackathon"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.4.0"
)
