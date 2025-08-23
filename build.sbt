name := "transactions-cruncher"

version := "0.1"

scalaVersion := "2.13.16"
val sparkVersion = "3.5.6"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "ch.qos.logback" % "logback-classic" % "1.5.18" ,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

fork := true

Test / parallelExecution := false

connectInput := true

// This is to make it work with Java 17+, a known issue
javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.xml/jdk.xml.internal=ALL-UNNAMED"
)

addCommandAlias("run1", "runMain cruncher.DailyTotalCalculator")
addCommandAlias("run2", "runMain cruncher.AvgByTypeCalculator")
addCommandAlias("run3", "runMain cruncher.RollingAvgCalculator")