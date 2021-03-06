name := "overflowdb"
ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.13.4"
publish/skip := true
enablePlugins(GitVersioning)

lazy val core = project.in(file("core"))
lazy val tinkerpop3 = project.in(file("tinkerpop3")).dependsOn(core % "compile->compile;test->test")

lazy val traversal = project.in(file("traversal"))
                       .dependsOn(core)
                       .dependsOn(tinkerpop3 % "test->test") // TODO drop this dependency - currently necessary for GratefulDeadTest which uses graphml loading

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

