package com.nykytenko

import cats.effect.Effect
import cats.implicits._
import pureconfig.error.ConfigReaderException

package object config {

  case class SparkConfig(name: String, master: String, windowSize: Int, sliceSize: Int,
                         eventRate: Int, clickViewRate: Int, categoriesRate: Int, batchSize: Int)

  case class KafkaConfig(host: String, groupId: String, topic: String)

  case class DbConfig(lookup: LookupConfig, registry: RegistryConfig)

  case class LookupConfig(host: String, port: Int, ttl: Long, name: String)

  case class RegistryConfig(host: String, port: Int, ttl: Long, name: String)

  case class AppConfig(spark: SparkConfig, kafka: KafkaConfig, db: DbConfig)

  import pureconfig._

  def load[F[_]](implicit E: Effect[F]): F[AppConfig] = E.delay {
    loadConfig[AppConfig]
  }.flatMap {
    case Right(config) => E.pure(config)
    case Left(e) => E.raiseError(new ConfigReaderException[AppConfig](e))
  }
}
