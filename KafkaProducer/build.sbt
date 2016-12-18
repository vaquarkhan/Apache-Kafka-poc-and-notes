name := "Kafka-Producer"

version := "1.0"

scalaVersion := "2.11.8"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

resolvers += "Staging repo" at "https://repository.apache" +
  ".org/content/repositories/orgapachespark-1216"

// enablePlugins(UniversalPlugin)

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.1"

fork := true

javaOptions ++= Seq("-Xmx15G", "-XX:MaxPermSize=3G")
  // "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0-xx1" % "provided"

// libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1" // % "provided"

// libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.5.1" // % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0-xx1" intransitive()
