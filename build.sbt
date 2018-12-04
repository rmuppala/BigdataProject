name := "NetworkWordCount"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies ++= {

  val procVer = "7.4.2"

  Seq(
    "org.clulab" %% "processors-main" % procVer,
    "org.clulab" %% "processors-corenlp" % procVer,
    "org.clulab" %% "processors-odin" % procVer,
    "org.clulab" %% "processors-modelsmain" % procVer,
    "org.clulab" %% "processors-modelscorenlp" % procVer,
    "org.apache.spark" %% "spark-core" % "2.1.0" exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark" %% "spark-sql" % "2.1.0",
    "org.apache.spark" % "spark-streaming_2.11" % "2.1.0",
    "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1",
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"
  )
}
