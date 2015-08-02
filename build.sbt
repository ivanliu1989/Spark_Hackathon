name := "Spark_Hackathon"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "1.4.0", //% "provided"
   "org.apache.spark" %% "spark-sql" % "1.4.0",
   "org.apache.spark" %% "spark-streaming" % "1.4.0",
   "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0",
   "org.apache.spark" %% "spark-mllib" % "1.4.0",
   "org.apache.commons" % "commons-lang3" % "3.0",
   "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",
   "org.scalatest" %% "scalatest" % "2.2.1" % "test",
   "com.databricks" % "spark-csv_2.11" % "1.0.3",
   "joda-time" % "joda-time" % "2.8.1",
   "org.joda"  % "joda-convert" % "1.7",
   "com.github.scopt" %% "scopt" % "3.3.0"
 )
 
resolvers += Resolver.sonatypeRepo("public")
resolvers ++= Seq(
   "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
   "Sonatype public" at "http://oss.sonatype.org/content/groups/public/",
   "Sonatype" at "http://nexus.scala-tools.org/content/repositories/public",
   "Scala Tools" at "http://scala-tools.org/repo-snapshots/",
   "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
   "Akka" at "http://akka.io/repository/",
   "JBoss" at "http://repository.jboss.org/nexus/content/groups/public/",
   "GuiceyFruit" at "http://guiceyfruit.googlecode.com/svn/repo/releases/"
 )

// set the main class for 'sbt run'
mainClass in (Compile, run) := Some("StreamingMachineLearning")




