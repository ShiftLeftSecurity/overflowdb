name := "overflowdb-formats"

libraryDependencies ++= Seq(
  "com.github.tototoshi" %% "scala-csv" % "1.4.1",
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
  "com.github.pathikrit" %% "better-files" % "3.9.2" % Test,
  "com.github.scopt" %% "scopt" % "4.1.0",
  "io.spray" %% "spray-json" % "1.3.6"
)

Test / console / scalacOptions -= "-Xlint"
