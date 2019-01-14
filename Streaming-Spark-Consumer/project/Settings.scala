import sbt.Def
import sbt.Keys._

object Settings {
  val common: Seq[Def.Setting[String]] = Seq(
    name         := "Streaming-Spark-Consumer",
    organization := "com.nykytenko",
    version      := "0.0.1-SNAPSHOT",
    scalaVersion := "2.11.12"
  )
}
