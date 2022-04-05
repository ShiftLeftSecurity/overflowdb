name := "overflowdb"
ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.13.8"
ThisBuild/crossScalaVersions := Seq("2.13.8", "3.1.2")
publish/skip := true

lazy val core        = project.in(file("core"))
lazy val testdomains = project.in(file("testdomains")).dependsOn(core)
lazy val formats     = project.in(file("formats"    )).dependsOn(testdomains)
lazy val traversal   = project.in(file("traversal"  )).dependsOn(formats)
lazy val coreTests   = project.in(file("core-tests" )).dependsOn(formats % Test)

ThisBuild/libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.36" % Test,
  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
)

ThisBuild/scalacOptions ++= Seq("-deprecation", "-feature") ++ (
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq(
          "-Xtarget:8"
          )
    case _ => Seq(
          "-target:jvm-1.8"
      )
  }
)

ThisBuild / compile / javacOptions ++= Seq(
  "-g", //debug symbols
  "--release", "8")

ThisBuild/Test/testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
ThisBuild/Test/fork := true

ThisBuild/resolvers ++= Seq(
  Resolver.mavenLocal,
  "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public")

ThisBuild/Compile/scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-language:implicitConversions",
  // "-language:existentials",
)

ThisBuild/publishTo := sonatypePublishToBundle.value
ThisBuild/scmInfo := Some(ScmInfo(url("https://github.com/ShiftLeftSecurity/overflowdb"),
                                      "scm:git@github.com:ShiftLeftSecurity/overflowdb.git"))
ThisBuild/homepage := Some(url("https://github.com/ShiftLeftSecurity/overflowdb/"))

ThisBuild/licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild/developers := List(
  Developer("mpollmeier", "Michael Pollmeier", "michael@michaelpollmeier.com", url("http://www.michaelpollmeier.com/")),
  Developer("ml86", "Markus Lottmann", "markus@shiftleft.io", url("https://github.com/ml86")))

Global/cancelable := true
Global/onChangedBuildSource := ReloadOnSourceChanges

