// note: this is an independent project with it's own subprojects
name := "benchmarks"

ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.13.0"
ThisBuild/publish/skip := true

lazy val root = project.in(file("."))
libraryDependencies ++= Seq(
  "org.apache.tinkerpop" % "gremlin-core" % "3.4.3",
  "org.slf4j" % "slf4j-nop" % "1.7.28",
)

lazy val overflowdb = project.in(file("overflowdb")).dependsOn(root)
lazy val tinkergraph = project.in(file("tinkergraph")).dependsOn(root)
lazy val janusgraph = project.in(file("janusgraph")).dependsOn(root)
lazy val orientdb = project.in(file("orientdb")).dependsOn(root)
lazy val neo4j = project.in(file("neo4j")).dependsOn(root)

ThisBuild/resolvers ++=
  Seq(Resolver.mavenLocal, "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public")
Global/cancelable := true
