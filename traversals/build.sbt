name := "overflowdb-traversals"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.28" % Test,
)

// execute tests in root project so that they work in sbt *and* intellij
Test/baseDirectory := (ThisBuild / Test / run / baseDirectory).value