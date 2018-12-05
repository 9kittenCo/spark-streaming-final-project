package com.nykytenko

import cats.effect.Effect
import com.nykytenko.config.SparkConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class SparkConfigService[F[_]]()(implicit E: Effect[F]) {
  def createFromConfig(conf: SparkConfig): F[(SparkSession, StreamingContext)] = E.delay {
    val ss = SparkSession.builder
      .master(conf.master)
      .appName(conf.name)
      .config("spark.driver.memory", "2g")
      .getOrCreate()
    val ssc = new StreamingContext(ss.sparkContext, Seconds(conf.batchSize))

    (ss, ssc)
  }

  def close(ss: SparkSession, ssc: StreamingContext): F[Unit] = E.delay {
    ssc.stop()
    ss.close()
  }
}
