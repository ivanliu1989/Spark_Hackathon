# Spark_Hackathon
Building a Streaming Machine Learning Algorithm for the Spark Hackathon hosted by Servian Ltd Pty <br>

sbt package <br>

/spark/bin/spark-submit --class "utilClasses.StreamingMachineLearning_Main" --master local[4] target/scala-2.11/spark_hackathon_2.11-1.0.jar ../data/trainHistory ../data/testHistory 3600 22
<br>

sbt 
<br>
~run-main utilClasses.StreamingMachineLearning_Main "../data/trainHistory" "../data/testHistory" 3600 22 
<br>