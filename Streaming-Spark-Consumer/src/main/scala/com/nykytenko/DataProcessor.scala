package com.nykytenko

import cats.effect.Effect
import com.nykytenko.HelperClasses._
import com.nykytenko.config.SparkConfig
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import cats.implicits._

case class DataProcessor[F[_]](conf: SparkConfig, ss: SparkSession)(implicit E: Effect[F]) extends Serializable {

  val schema: StructType = StructType(Seq(
    StructField("unix_time"   , TimestampType , nullable = true),
    StructField("category_id" , IntegerType   , nullable = true),
    StructField("ip"          , StringType    , nullable = true),
    StructField("type"        , StringType    , nullable = true)
  ))

  val aggregateFunc: (EventCount, EventCount) => EventCount = {
    case (e1, e2) => EventCount(
                                ip = e1.ip,
                                events = e1.events + e2.events,
                                clicks = e1.clicks + e2.clicks,
                                views = e1.views + e2.views,
                                category_ids = e1.category_ids ++ e2.category_ids
                              )
}

  def process1()(stream: DStream[(Ip, EventCount)]): F[DStream[(Ip, EventCount)]] = E.delay {
    stream
      .reduceByKeyAndWindow(aggregateFunc, Minutes(10), Seconds(60))
      .cache()
  }

  def process2()(stream: Dataset[EventCountWithTimestamp]): F[Dataset[EventCount]] = E.delay {
    import ss.implicits._

    stream
      .withWatermark("unix_time", s"${conf.windowSize} seconds")
      .groupBy($"ip",
        window($"unix_time", s"${conf.windowSize} minutes", s"${conf.sliceSize} minutes")
      )
      .agg(
        count("*").as("events"),
        sum("clicks").as("clicks"),
        sum("views").as("views"),
        collect_set("category_ids").as("category_ids")
      )
      .as[EventCount]
  }
}