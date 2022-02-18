name := "overflowdb-core-tests"

publish/skip := true

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.36" % Test,
)

Test/testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
