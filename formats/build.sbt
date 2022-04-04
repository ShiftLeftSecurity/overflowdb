name := "formats"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
)
scalacOptions ++= Seq("-deprecation:false")

Test/console/scalacOptions -= "-Xlint"
