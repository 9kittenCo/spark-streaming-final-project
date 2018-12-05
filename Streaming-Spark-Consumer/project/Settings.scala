import sbt.Keys._

object Settings {
  val common = Seq(
    name         := "Streaming-Spark-Consumer",
    organization := "com.nykytenko",
    version      := "0.0.1-SNAPSHOT",
    scalaVersion := "2.12.7"
  )
}
