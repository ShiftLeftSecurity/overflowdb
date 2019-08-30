name := "overflowdb-tinkerpop3"

val tinkerpopVersion = "3.4.3"

libraryDependencies ++= Seq(
  "org.apache.tinkerpop" % "gremlin-core" % tinkerpopVersion,
  "org.apache.commons" % "commons-lang3" % "3.9",
  "net.sf.trove4j" % "core" % "3.1.0",
  "org.msgpack" % "msgpack-core" % "0.8.17",
  "com.h2database" % "h2-mvstore" % "1.4.199",
  "org.apache.tinkerpop" % "gremlin-test" % tinkerpopVersion % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.28" % Test,
)

Test/testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
Test/compile/javacOptions ++= Seq("-g")
Test/fork := true
scalacOptions ++= Seq("-deprecation", "-feature")
