name := "overflowdb-traversals"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.28" % Test,
)

Test/compile/javacOptions ++= Seq("-g")
Test/fork := true
scalacOptions ++= Seq("-deprecation", "-feature")
