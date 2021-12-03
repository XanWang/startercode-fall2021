lazy val root = (project in file("."))
  .settings(
    name := "hw6q1",
    version := "1.0",
    scalaVersion := "2.11.12"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "compile",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "compile"
)