import sbt._
import sbt.internal.ProjectMatrix

// One axis value per supported Spark line. `dir` selects the version-specific source directory
// (scala-spark-<dir>) so API divergences stay isolated from the shared sources.
case class SparkAxis(idSuffix: String, directorySuffix: String, version: String, dir: String)
    extends VirtualAxis.WeakAxis
