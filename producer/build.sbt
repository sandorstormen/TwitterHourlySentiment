name := "movie-sentiment-producer"

version := "1.0"

scalaVersion := "2.13.1"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq(
  "com.danielasfregola" %% "twitter4s" % "6.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.kafka" % "kafka_2.11" % "1.0.0"
)
