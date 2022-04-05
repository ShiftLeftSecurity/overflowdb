name := "formats"

libraryDependencies ++= Seq(
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
)
scalacOptions ++= Seq("-deprecation:false")

Test/console/scalacOptions -= "-Xlint"
