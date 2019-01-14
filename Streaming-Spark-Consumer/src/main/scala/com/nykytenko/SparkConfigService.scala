package com.nykytenko

import cats.effect.Effect
import com.nykytenko.config.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class SparkConfigService[F[_]]()(implicit E: Effect[F]) {
  def createFromConfig(conf: AppConfig): F[(SparkSession, StreamingContext)] = E.delay {
    val ss = SparkSession.builder
      .master(conf.spark.master)
      .appName(conf.spark.name)
      .config("spark.driver.memory", "2g")
      .config("redis.host", conf.db.registry.host)
      .config("redis.port", conf.db.registry.port)
      .getOrCreate()
    val ssc = new StreamingContext(ss.sparkContext, Seconds(conf.spark.batchSize))
    ss.sparkContext.setLogLevel("ERROR")

    (ss, ssc)
  }

  def start(ssc: StreamingContext): F[Unit] = E.delay{
    //ssc.start()
    ssc.awaitTermination()
  }
  def close(ss: SparkSession, ssc: StreamingContext): F[Unit] = E.delay {
    ss.close()
  }
}
