// note: this is an independent project with it's own subprojects
name := "benchmarks"

ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.13.0"
ThisBuild/publish/skip := true

lazy val root = project.in(file("."))
lazy val overflowdb = project.in(file("overflowdb")).dependsOn(root)
lazy val tinkergraph = project.in(file("tinkergraph")).dependsOn(root)
lazy val janusgraph = project.in(file("janusgraph")).dependsOn(root)

ThisBuild/resolvers ++=
  Seq(Resolver.mavenLocal, "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public")
Global/cancelable := true
