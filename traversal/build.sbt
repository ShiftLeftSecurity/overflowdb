name := "overflowdb-traversal"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.reflections" % "reflections" % "0.9.12",
  "com.massisframework" % "j-text-utils" % "0.3.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.slf4j" % "slf4j-simple" % "1.7.28" % Test,
)

Test/console/scalacOptions -= "-Xlint"
Test/console/initialCommands :=
  """|import io.shiftleft.overflowdb.traversal._
     |import io.shiftleft.overflowdb.traversal.testdomains.gratefuldead._
     |val gd = GratefulDead.traversal(GratefulDead.newGraphWithData)""".stripMargin
