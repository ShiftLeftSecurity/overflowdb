name := "overflowdb-tinkerpop3"

libraryDependencies ++= Seq(
  "org.apache.tinkerpop" % "gremlin-core" % "3.3.4",
  "org.apache.commons" % "commons-lang3" % "3.3.1",
  "net.sf.trove4j" % "core" % "3.1.0",
  "org.msgpack" % "msgpack-core" % "0.8.16",
  "com.h2database" % "h2-mvstore" % "1.4.199",
  "org.apache.tinkerpop" % "gremlin-test" % "3.3.4" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.21" % Test,
)

Test/testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
Test/compile/javacOptions ++= Seq("-g")
Test/fork := true
scalacOptions ++= Seq("-deprecation", "-feature")
