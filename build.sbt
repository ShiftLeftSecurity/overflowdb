name := "overflowdb-root"
ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.12.8"
publish / skip := true

lazy val tinkerpop3 = project.in(file("tinkerpop3"))

// we only use scala in src/test
ThisBuild/autoScalaLibrary := false
ThisBuild/crossPaths := false

ThisBuild/resolvers ++= Seq(Resolver.mavenLocal,
                            "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public")

ThisBuild/publishTo := sonatypePublishTo.value
ThisBuild/scmInfo := Some(ScmInfo(url("https://github.com/ShiftLeftSecurity/overflowdb"),
                                      "scm:git@github.com:ShiftLeftSecurity/overflowdb.git"))
ThisBuild/homepage := Some(url("https://github.com/ShiftLeftSecurity/overflowdb/"))
ThisBuild/licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild/developers := List(
  Developer("mpollmeier", "Michael Pollmeier", "michael@michaelpollmeier.com", url("http://www.michaelpollmeier.com/")),
  Developer("ml86", "Markus Lottmann", "markus@shiftleft.io", url("https://github.com/ml86")))

// allow to cancel sbt compilation/test/... using C-c
Global/cancelable := true
