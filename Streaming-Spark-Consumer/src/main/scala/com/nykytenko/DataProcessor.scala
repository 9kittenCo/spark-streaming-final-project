package com.nykytenko

import java.sql.Timestamp
import java.time.LocalDateTime

import cats.effect.Effect
import com.nykytenko.HelperClasses._
import com.nykytenko.config.SparkConfig
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec}
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
  def roundTimestamp(t: Timestamp):LocalDateTime = {
    val dt = t.toLocalDateTime
    val m = dt.getMinute % conf.windowSize.toInt
    dt.minusMinutes(m).minusSeconds(dt.getSecond)
  }

  def stateUpdateFunction(key: (Ip, LocalDateTime), value: Option[EventCount], state: State[EventCount]): Option[((Ip, LocalDateTime), EventCount)] = {
    (value, state.getOption()) match {
      case (Some(e1), None) =>
        // the 1st visit
        state.update(EventCount(e1.ip, e1.events, e1.clicks, e1.views, e1.category_ids))
        None
      case (Some(e1), Some(e2)) =>
        // next visit
        state.update(EventCount(
          ip = e1.ip,
          events = e1.events + e2.events,
          clicks = e1.clicks + e2.clicks,
          views = e1.views + e2.views,
          category_ids = e1.category_ids ++ e2.category_ids
        ))
        None
      case (None, Some(e2)) => Some(key, e2)
      case _ => None
    }
  }

  def process1()(stream: DStream[LogEvent]): F[DStream[((Ip, LocalDateTime), EventCount)]] = E.delay {
    val pairedDstream = stream.map(event => ((event.ip, roundTimestamp(event.unix_time)),event.toEventCount))
    val r = pairedDstream
      .reduceByKey(aggregateFunc)
      .mapWithState {
        StateSpec.function(stateUpdateFunction _)
          .timeout(Minutes(10))
      }
      .map(m => m.get)
    //ssc.checkpoint(checkpointDirectory)
    r.cache()
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