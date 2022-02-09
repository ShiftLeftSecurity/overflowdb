name := "overflowdb-tinkerpop3"

val tinkerpopVersion = "3.4.12"

libraryDependencies ++= Seq(
  "org.apache.tinkerpop" % "gremlin-core" % tinkerpopVersion,
  "org.apache.tinkerpop" % "gremlin-test" % tinkerpopVersion % Test,
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.36" % Test,
)

scalacOptions ++= Seq("-deprecation:false")
/* it's a java-only build */
autoScalaLibrary := false
crossPaths := false
