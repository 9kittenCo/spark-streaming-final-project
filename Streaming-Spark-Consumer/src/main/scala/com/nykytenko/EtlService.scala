package com.nykytenko

import cats.effect.Effect
import com.nykytenko.HelperClasses._
import com.nykytenko.config.AppConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

case class EtlResult(value: EtlDescription, ss: SparkSession, ssc: StreamingContext)

object EtlService {
  def getEtl[F[_]](stype: StreamingType, appConf: AppConfig, ssc: StreamingContext, ss: SparkSession)(implicit E: Effect[F]): EtlDescription = {
    val ds = new EtlDescription1(
      ssc = ssc,
      kafkaStream = KafkaService(appConf.kafka).createDStreamFromKafka(ssc),
      process     = DataProcessor(appConf.spark, ss).process1(),
      write       = DbService(appConf, ss).persist1()
    )

    val strs = new EtlDescription2(
      ssc         = ssc,
      kafkaStream = KafkaService(appConf.kafka).createStreamFromKafka(ss),
      process     = DataProcessor(appConf.spark, ss).process2(),
      write       = DbService(appConf, ss).persist2()
    )

    stype match {
      case DStreaming          => ds
      case StructuredStreaming => strs
      case _                   => ds
    }
  }
}

sealed trait EtlDescription {
  def process[F[_]]()(implicit E: Effect[F]): F[Unit]
}

class EtlDescription1(
                       ssc: StreamingContext,
                       kafkaStream: InputDStream[ConsumerRecord[String, String]],
                       process: InputDStream[ConsumerRecord[String, String]] => DStream[(Ip, EventCount)],
                       write: DStream[(Ip, EventCount)] => Unit
                     ) extends EtlDescription {
  override def process[F[_]]()(implicit E: Effect[F]): F[Unit] = E.delay {
    ssc.start()
    ssc.awaitTermination()

    write(process(kafkaStream))

    ssc.stop()
  }
}

class EtlDescription2(
                       ssc: StreamingContext,
                       kafkaStream: DataFrame,
                       process: DataFrame => Dataset[EventCount],
                       write: Dataset[EventCount] => Unit
                     ) extends EtlDescription {
  override def process[F[_]]()(implicit E: Effect[F]): F[Unit] = E.delay {
    ssc.start()
    ssc.awaitTermination()

    write(process(kafkaStream))

    ssc.stop()
  }
}

