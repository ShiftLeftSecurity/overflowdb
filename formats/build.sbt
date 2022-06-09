name := "overflowdb-formats"

libraryDependencies ++= Seq(
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
  ("com.github.pathikrit" %% "better-files" % "3.9.1" % Test).cross(CrossVersion.for3Use2_13),
  "com.github.scopt" %% "scopt" % "4.0.1",
)
scalacOptions ++= Seq("-deprecation:false")

Test/console/scalacOptions -= "-Xlint"
