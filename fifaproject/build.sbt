val scala3Version = "3.2.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "FifaProject",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
        ("org.apache.spark" %% "spark-sql" % "3.2.0" % "provided").cross(CrossVersion.for3Use2_13),
    )
  )

  Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
