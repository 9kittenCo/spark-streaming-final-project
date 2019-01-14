package com.nykytenko

import java.sql.Timestamp

import cats.effect.Effect
import com.nykytenko.HelperClasses.{EventCount, EventCountWithTimestamp, Ip, LogEvent}
import com.nykytenko.config.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, LocationStrategy}
import cats.implicits._
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.generic.auto._

case class KafkaService[F[_]](config: KafkaConfig)(implicit E: Effect[F]) extends Serializable {

  private val preferredHosts: LocationStrategy = LocationStrategies.PreferConsistent

  private lazy val schema: StructType = StructType(
    Seq(
      StructField("unix_time"   , TimestampType),
      StructField("category_id" , IntegerType),
      StructField("ip"          , StringType),
      StructField("type"        , StringType)
    )
  )

  private lazy val schemaTop = StructType(
    Seq(
      StructField(name = "schema", dataType = DataTypes.createMapType(StringType, StringType), nullable = false),
      StructField(name = "payload", dataType = StringType, nullable = false)
    )
  )


  private val properties = {
    Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> config.host,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG                 -> config.groupId,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       -> (false: java.lang.Boolean)
    )
  }

  def createDStreamFromKafka(ssc: StreamingContext): F[DStream[(Ip, EventCount)]] = E.delay {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](List(config.topic), properties)
    ).map { x =>
      implicit val eventCountDecoder: Decoder[EventCount] = deriveDecoder[EventCount]
      implicit val TimestampFormat : Encoder[Timestamp] with Decoder[Timestamp] = new Encoder[Timestamp] with Decoder[Timestamp] {
        override def apply(a: Timestamp): Json = Encoder.encodeLong.apply(a.getTime)

        override def apply(c: HCursor): Result[Timestamp] = Decoder.decodeLong.map(s => new Timestamp(s)).apply(c)
      }
      x.value().asJson.as[LogEvent].toOption
    }.filter(_.isDefined)
      .map(elOpt => (elOpt.get.ip, elOpt.get.toEventCount))
  }

  def createStreamFromKafka(ss: SparkSession): F[Dataset[EventCountWithTimestamp]] = E.delay {
    import ss.implicits._
    ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.host)
      .option("subscribe", config.topic)
      .load
      .select("value")
      .as[String]
      .select(from_json(decode($"value", "UTF-8"), schemaTop).as("parsed_value"))
      .select(from_json($"parsed_value.payload", schema).as("parsed_json"))
      .select("parsed_json.*")
      .as[LogEvent]
      .map(x => x.toEventCountWithTimestamp)
  }
}
