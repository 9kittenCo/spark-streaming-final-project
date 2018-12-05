package com.nykytenko

import com.nykytenko.HelperClasses.{EventCount, Ip, LogEvent}
import com.nykytenko.config.SparkConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds}
import io.circe.syntax._
import io.circe.generic.auto._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class DataProcessor(conf: SparkConfig, ss: SparkSession) {

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

  def process1()(stream: InputDStream[ConsumerRecord[String, String]]): DStream[(Ip, EventCount)] = {
    stream.map { s =>
      s.value().asJson.as[LogEvent].toOption
    }
      .filter(_.isDefined).map(elOpt => (elOpt.get.ip, elOpt.get.toEventCount))
      .reduceByKeyAndWindow(aggregateFunc, Minutes(10), Seconds(60))
      .cache()
  }

  def process2()(stream: DataFrame): Dataset[EventCount] = {
    import ss.implicits._

    stream
      .select("value")
      .as[String]
      .filter(x => !x.isEmpty)
      .select(from_json($"value".cast(StringType), schema).as("parsed_json"))
      .select("parsed_json.*")
      .as[LogEvent]
      .map(x => x.toEventCount)
      .withWatermark("unix_time", s"${conf.windowSize} seconds")
      .groupBy(
        window($"unix_time", "10 minutes", "5 minutes"))
      .agg(
        count("*").as("events"),
        sum("clicks").as("clicks"),
        sum("views").as("views"),
        collect_set("category_ids").as("category_ids")
      )
      .as[EventCount]
  }
}