import sbt._
import sbt.Keys._
import scala.util.Properties

// sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._

object Version {
  val scala       = "2.10.5"
  val spark       = "1.4.0"

  val geotrellis  = "0.10.0-SNAPSHOT"
}

object Build extends Build {
  // Default settings
  override lazy val settings =
    super.settings ++
  Seq(
    version := "0.1.0",
    scalaVersion := Version.scala,
    organization := "com.packt",

    scalacOptions ++=
      Seq(
        "-deprecation",
        "-feature",
        "-language:implicitConversions"
        ),

    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
  )

  // Project: root
  lazy val root =
    Project("root", file("."))
      .settings(rootSettings:_*)

  lazy val rootSettings =
    Seq(
      organization := "com.packt",
      name := "learning-spark",

      scalaVersion := Version.scala,

      // raise memory limits here if necessary
      javaOptions += "-Xmx2G",

      fork := true,
      connectInput in run := true,

      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % Version.spark,
        "com.opencsv" % "opencsv"      % "3.4",
        "com.github.nscala-time" %% "nscala-time" % "1.6.0",
        "com.azavea.geotrellis" %% "geotrellis-raster" % Version.geotrellis,
        "de.javakaffee" % "kryo-serializers" % "0.33",
        "org.scalatest" %%  "scalatest" % "2.2.0" % "test"
      ),

      test in assembly := {}
    )  ++ net.virtualvoid.sbt.graph.Plugin.graphSettings
}
