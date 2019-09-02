name := "spark-analysis"

version := "1.0"

// We have to use this scala version to build because Spark packages support 2.11 scala only
scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.6.10",
  "com.typesafe.play" %% "play-functional" % "2.6.10"
)
