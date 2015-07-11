import sbt._
import sbt.Keys._
import scala.util.Properties

// sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._

object Version {
  def either(environmentVariable: String, default: String): String =
    Properties.envOrElse(environmentVariable, default)

  val geotrellis  = "0.10.0-SNAPSHOT"
  val scala       = "2.10.5"
  val spray       = "1.2.2"
  val akka        = "2.3.9"
  lazy val hadoop      = either("SPARK_HADOOP_VERSION", "2.5.0")
  lazy val spark       = "1.4.0"

}

object PPABuild extends Build {
  val resolutionRepos = Seq(
    "Local Maven Repository"  at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    "Typesafe Repo"           at "http://repo.typesafe.com/typesafe/releases/",
    "spray repo"              at "http://repo.spray.io/",
    "snapshots"     at "https://oss.sonatype.org/content/repositories/snapshots"
  )

  // Default settings
  override lazy val settings =
    super.settings ++
  Seq(
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
    version := "0.0.1",
    scalaVersion := Version.scala,
    organization := "com.packt",

    // disable annoying warnings about 2.10.x
    conflictWarning in ThisBuild := ConflictWarning.disable,
    scalacOptions ++=
      Seq("-deprecation",
        "-unchecked",
        "-Yinline-warnings",
        "-language:implicitConversions",
        "-language:reflectiveCalls",
        "-language:higherKinds",
        "-language:postfixOps",
        "-language:existentials",
        "-feature"),

    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),

    test in assembly := {}
  )

  // Project: root
  lazy val root =
    Project("explorer", file("."))
      .settings(rootSettings:_*)

  lazy val rootSettings =
    Seq(
      organization := "com.packt",
      name := "parking-violations-explorer",

      scalaVersion := Version.scala,

      fork := true,
      connectInput in run := true,

      mainClass in (Compile, run) := Some("com.packt.spark.chapter2.Accumulators"),

      // raise memory limits here if necessary
      javaOptions += "-Xmx2G",

      libraryDependencies ++= Seq(
        "com.opencsv" % "opencsv"      % "3.4",
        "com.github.nscala-time" %% "nscala-time" % "1.6.0",
        "com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis
          exclude("org.apache.accumulo", "accumulo-core"),
        "org.apache.spark" %% "spark-core" % Version.spark,
        "org.apache.hadoop" % "hadoop-client" % Version.hadoop % "provided",
        "org.scalatest" %%  "scalatest" % "2.2.0" % "test"
      )
    )  ++
  net.virtualvoid.sbt.graph.Plugin.graphSettings

}
