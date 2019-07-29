name := "overflowdb-root"
ThisBuild/organization := "io.shiftleft"
ThisBuild/scalaVersion := "2.12.8"
publish / skip := true

lazy val tinkerpop3 = project.in(file("tinkerpop3"))

// we only use scala in src/test
ThisBuild/autoScalaLibrary := false
ThisBuild/crossPaths := false

ThisBuild/coursierTtl := Some(scala.concurrent.duration.Duration.create("5 min"))
ThisBuild/resolvers ++= Seq(
  Resolver.mavenLocal,
  "Artifactory release local" at "https://shiftleft.jfrog.io/shiftleft/libs-release-local",
  "Apache public" at "https://repository.apache.org/content/groups/public/",
  "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public"
)

ThisBuild/publishTo := Some("releases" at "https://shiftleft.jfrog.io/shiftleft/libs-release-local")
ThisBuild/publishMavenStyle := true

// allow to cancel sbt compilation/test/... using C-c
Global/cancelable := true
