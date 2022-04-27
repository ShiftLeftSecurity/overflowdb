name := "overflowdb-traversal-tests"

publish/skip := true

scalacOptions ++= Seq("-deprecation:false")

Test/console/scalacOptions -= "-Xlint"
