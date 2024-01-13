name := "overflowdb"
ThisBuild / organization := "io.shiftleft"
ThisBuild / scalaVersion := "2.13.10"
ThisBuild / crossScalaVersions := Seq("2.13.10", "3.3.0")
// TODO once we're on Scala 3.2.2: make chained implicits in `Implicits.scala` available again
// also, change other places that have temporarily been adapted - search for `TODO Scala 3.2.2`
publish / skip := true

lazy val core = project.in(file("core"))
lazy val testdomains = project.in(file("testdomains")).dependsOn(core)
lazy val traversal = project.in(file("traversal")).dependsOn(core)
lazy val formats = project.in(file("formats")).dependsOn(traversal, testdomains % Test)
lazy val coreTests = project.in(file("core-tests")).dependsOn(formats, testdomains)
lazy val traversalTests = project.in(file("traversal-tests")).dependsOn(formats)

ThisBuild / libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-simple" % "2.0.11" % Test,
  "org.scalatest" %% "scalatest" % "3.2.16" % Test
)

ThisBuild / scalacOptions ++= Seq(
  "-release",
  "8",
  "-deprecation",
  "-feature"
)

ThisBuild / compile / javacOptions ++= Seq(
  "-g", // debug symbols
  "--release",
  "8"
)

ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
ThisBuild / Test / fork := true

ThisBuild / resolvers ++= Seq(
  Resolver.mavenLocal,
  "Sonatype OSS".at("https://oss.sonatype.org/content/repositories/public")
)

ThisBuild / Compile / scalacOptions ++= Seq(
  "-language:implicitConversions"
  // "-language:existentials",
)

ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/ShiftLeftSecurity/overflowdb"), "scm:git@github.com:ShiftLeftSecurity/overflowdb.git")
)
ThisBuild / homepage := Some(url("https://github.com/ShiftLeftSecurity/overflowdb/"))

ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer("mpollmeier", "Michael Pollmeier", "michael@michaelpollmeier.com", url("http://www.michaelpollmeier.com/")),
  Developer("ml86", "Markus Lottmann", "markus@shiftleft.io", url("https://github.com/ml86"))
)

Global / cancelable := true
Global / onChangedBuildSource := ReloadOnSourceChanges
