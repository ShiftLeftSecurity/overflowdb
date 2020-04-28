name := "overflowdb"
ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.13.2"
publish/skip := true
enablePlugins(GitVersioning)

lazy val tinkerpop3 = project.in(file("tinkerpop3"))
lazy val traversal = project.in(file("traversal")).dependsOn(tinkerpop3) //TODO factor out `core` from tinkerpop3

ThisBuild/resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.bintrayRepo("shiftleft", "maven"),
  "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public")

ThisBuild/Compile/scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-feature",
  "-deprecation",
  "-language:implicitConversions",
  // "-language:existentials",
)

ThisBuild/bintrayVcsUrl := Some("https://github.com/ShiftLeftSecurity/overflowdb")
ThisBuild/licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

Global/cancelable := true
Global/onChangedBuildSource := ReloadOnSourceChanges

