ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.10"

// Dependency Versions
val sparkVersion = "3.0.2"
val postgresVersion = "42.2.2"
val cassandraConnectorVersion = "3.0.0"
val akkaVersion = "2.5.24"
val akkaHttpVersion = "10.1.7"
val twitter4jVersion = "4.0.7"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.4.1"
val nlpLibVersion = "3.5.1"

resolvers ++= Seq(
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,

  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  "org.postgresql" % "postgresql" % postgresVersion,

  "org.twitter4j" % "twitter4j-core" % twitter4jVersion,
  "org.twitter4j" % "twitter4j-stream" % twitter4jVersion,

  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion classifier "models",

  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)
