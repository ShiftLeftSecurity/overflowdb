name := "overflowdb"
ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.13.0"
publish/skip := true
enablePlugins(GitVersioning)

lazy val tinkerpop3 = project.in(file("tinkerpop3"))
lazy val traversals = project.in(file("traversals")).dependsOn(tinkerpop3) //TODO factor out `core` from tinkerpop3

ThisBuild/resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.bintrayRepo("shiftleft", "maven"),
  "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public")

ThisBuild/bintrayVcsUrl := Some("https://github.com/ShiftLeftSecurity/overflowdb")
ThisBuild/licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

// allow to cancel sbt compilation/test/... using C-c
Global/cancelable := true
