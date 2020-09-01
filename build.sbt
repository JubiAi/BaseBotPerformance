name := "basebotstream"

version := "1.0"

organization := "ai.jubi"

scalaVersion := "2.12.3"
val sparkVersion = "3.0.0-preview"
val kafkaVersion = "2.2.1"
val mongoVersion = "2.4.1"

libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % mongoVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-token-provider-kafka-0-10" % sparkVersion % "provided"
libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.6.2" % "provided"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}