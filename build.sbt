name := "TwitterVisualizationProject"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" exclude ("log4j", "log4j")
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
//libraryDependencies += "org.elasticsearch" % "elasticsearch" % "6.2.1"
//libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.2.1"
libraryDependencies += "org.json" % "json" % "20170516"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.0.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.0"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.2.0"








lazy val defaultSettings = Defaults.coreDefaultSettings ++ Seq(
  resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
)

