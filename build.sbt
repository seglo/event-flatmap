lazy val `event-flatmap` = project.in(file("."))
  .settings(
    name := "EventFlatmap",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.0",
      "org.json4s" %% "json4s-native" % "3.5.2"
    )
  )
