package com.nykytenko

import cats.effect.Effect
import com.nykytenko.config.KafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, LocationStrategy}

case class KafkaService(config: KafkaConfig) {

  private val preferredHosts: LocationStrategy = LocationStrategies.PreferConsistent

  private val properties = {
    Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> config.host,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG                 -> config.groupId,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       -> (false: java.lang.Boolean)
    )
  }

  def createDStreamFromKafka[F[_]](ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(config.topic), properties)
      )
  }
  def createStreamFromKafka[F[_]](ss: SparkSession)(implicit E: Effect[F]): DataFrame = {
    ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.host)
      .option("subscribe", config.topic)
      .load()
  }
}
