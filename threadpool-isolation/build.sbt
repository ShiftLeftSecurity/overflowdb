name := "threadpool-isolation"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.28",
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.28" % Test
)

/* it's a java-only build */
autoScalaLibrary := false
crossPaths := false

Test/testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
