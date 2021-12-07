name := "overflowdb-traversal"

libraryDependencies ++= Seq(
  "net.oneandone.reflections8" % "reflections8" % "0.11.7", // go back to reflections once 0.9.13 is released
  "com.massisframework" % "j-text-utils" % "0.3.4",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.28" % Test,
)
scalacOptions ++= Seq("-deprecation:false")

Test/console/scalacOptions -= "-Xlint"
