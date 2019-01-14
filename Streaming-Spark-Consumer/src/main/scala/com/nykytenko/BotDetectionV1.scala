package com.nykytenko

import cats.effect.Effect
import cats.implicits._
import com.nykytenko.HelperClasses.BotDetection

case object BotDetectionV1 extends BotDetection {
  def program[F[_]]()(implicit E:Effect[F]): F[Unit] = {
    for {
      appConf     <- config.load[F]
      spark       <- SparkConfigService[F].createFromConfig(appConf)
      kafkaStream <- KafkaService[F](appConf.kafka).createDStreamFromKafka(spark._2)
      stream      <- DataProcessor[F](appConf.spark, spark._1).process1()(kafkaStream)
      _           = DbService.persist1()(stream)(appConf, spark._1, spark._2)
      _           <- SparkConfigService[F].start(spark._2)
    } yield ()
  }
}