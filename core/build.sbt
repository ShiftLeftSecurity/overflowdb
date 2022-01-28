name := "overflowdb-core"

libraryDependencies ++= Seq(
  "net.sf.trove4j" % "core" % "3.1.0",
  "org.msgpack" % "msgpack-core" % "0.9.0",
  "com.h2database" % "h2-mvstore" % "1.4.200",
  "org.slf4j" % "slf4j-api" % "1.7.35",
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.35" % Test,
)

/* it's a java-only build */
autoScalaLibrary := false
crossPaths := false

Test/testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
