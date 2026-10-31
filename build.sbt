val scala212 = "2.12.20"
val scala213 = "2.13.16"

val spark35 = SparkAxis("_spark35", "-spark35", "3.5.9", "3.5")
val spark40 = SparkAxis("_spark40", "-spark40", "4.0.5", "4")
val spark43 = SparkAxis("_spark43", "-spark43", "4.3.0", "4")

// Spark needs these on JDK 17+; harmless on older JDKs thanks to IgnoreUnrecognizedVMOptions.
val sparkJvmOptions = Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

def sparkRow(axis: SparkAxis)(p: Project): Project =
  p.settings(
    libraryDependencies += "org.apache.spark" %% "spark-sql" % axis.version % Provided,
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    Compile / unmanagedSourceDirectories +=
      (LocalRootProject / baseDirectory).value / "core" / "src" / "main" / s"scala-spark-${axis.dir}",
    Test / unmanagedSourceDirectories +=
      (LocalRootProject / baseDirectory).value / "core" / "src" / "test" / s"scala-spark-${axis.dir}",
    // 3.5 artifacts stay loadable on Java 8; Spark 4.x requires 17.
    scalacOptions ++= (if (axis.dir == "3.5") Seq("-release:8") else Seq("-release:17")),
    Test / fork := true,
    Test / javaOptions ++= sparkJvmOptions,
    Test / parallelExecution := false
  )

lazy val core = (projectMatrix in file("core"))
  .settings(name := "datamocker")
  .customRow(
    scalaVersions = Seq(scala212, scala213),
    axisValues = Seq(spark35, VirtualAxis.jvm),
    sparkRow(spark35)(_)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(spark40, VirtualAxis.jvm),
    sparkRow(spark40)(_)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(spark43, VirtualAxis.jvm),
    sparkRow(spark43)(_)
  )

lazy val root = (project in file("."))
  .aggregate(core.projectRefs: _*)
  .settings(publish / skip := true)
