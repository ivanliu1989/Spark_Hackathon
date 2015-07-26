name := "Spark_Hackathon"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.4.0"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "1.4.0"
)

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")