name := "overflowdb-core"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-lang3" % "3.9",
  "net.sf.trove4j" % "core" % "3.1.0",
  "org.msgpack" % "msgpack-core" % "0.8.17",
  "com.h2database" % "h2-mvstore" % "1.4.199",
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.28" % Test,
)

/* it's a java-only build */
autoScalaLibrary := false
crossPaths := false

Test/testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
Test/compile/javacOptions ++= Seq("-g", "-target", "1.8")
Test/fork := true
scalacOptions ++= Seq("-deprecation", "-feature", "-target:jvm-1.8")
javacOptions ++= Seq("-source", "1.8")
