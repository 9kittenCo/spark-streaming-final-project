import sbt._

object Dependencies {
  object Version {
    val PureConfig = "0.9.1"
    val Cats       = "1.0.1"
    val CatsEffect = "1.1.0"
    val Spark      = "2.4.0"
    val Kafka      = "2.1.0"
//    val Slf4j      = "1.7.25"
    val Circe      = "0.10.1"
  }

  val baseDependencies: Seq[ModuleID] = Seq(
    "com.github.pureconfig" %% "pureconfig"                 % Version.PureConfig,

    "org.typelevel"         %% "cats-effect"                % Version.CatsEffect,

//    "net.liftweb"           %% "lift-json"                  % "3.3.0",
    
    "io.circe"              %% "circe-core"                 % Version.Circe,
    "io.circe"              %% "circe-generic"              % Version.Circe,
    "io.circe"              %% "circe-generic-extras"       % Version.Circe,
 //   "io.circe"              %% "circe-parser"               % Version.Circe,
 //   "org.slf4j"             % "slf4j-simple"                % Version.Slf4j,
    "org.apache.kafka"      % "kafka-clients"               % Version.Kafka,

    "com.redislabs"         % "spark-redis"                 % "2.3.0",
    
    //redis sink
    "net.debasishg"         %% "redisclient"                % "3.8",
    "org.apache.spark"      %% "spark-sql-kafka-0-10"       % Version.Spark,
    "org.apache.spark"      %% "spark-streaming-kafka-0-10" % Version.Spark,
    "org.apache.spark"      %% "spark-core"                 % Version.Spark,
    "org.apache.spark"      %% "spark-sql"                  % Version.Spark,
    "org.apache.spark"      %% "spark-streaming"            % Version.Spark
  )
}
