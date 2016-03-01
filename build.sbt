name := "FullFlightData"

organization := "org.myorg.spark"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion = "1.6.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")


// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.google.code.gson" % "gson" % "2.3.1",
  "commons-cli" % "commons-cli" % "1.2",
  "com.databricks" % "spark-csv_2.11" % "1.3.0",
  "com.databricks" % "spark-avro_2.11" % "2.0.1"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
    case "log4j.properties"                                  => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case "reference.conf"                                    => MergeStrategy.concat
    case _                                                   => MergeStrategy.first
  }

scalacOptions += "-deprecation"



