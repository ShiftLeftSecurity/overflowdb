name := "formats"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
)
scalacOptions ++= Seq("-deprecation:false")

Test/console/scalacOptions -= "-Xlint"
