package com.nykytenko

import cats.effect.Effect
import cats.implicits._
import com.nykytenko.HelperClasses.BotDetection

case object BotDetectionV2 extends BotDetection {
  def program[F[_]]()(implicit E:Effect[F]): F[Unit] = {
    for {
      appConf     <- config.load[F]
      spark       <- SparkConfigService[F].createFromConfig(appConf)
      kafkaStream <- KafkaService[F](appConf.kafka).createStreamFromKafka(spark._1)
      stream      <- DataProcessor[F](appConf.spark, spark._1).process2()(kafkaStream)
      _           = DbService.persist2()(stream)(appConf, spark._1, spark._2)
      _           <- SparkConfigService[F].start(spark._2)
    } yield ()
  }
}