package com.nykytenko

import java.sql._

import cats.effect.Effect
import com.redislabs.provider.redis.toRedisContext
import com.nykytenko.HelperClasses.{EventCount, Ip}
import com.nykytenko.config.AppConfig
import com.redis.{RedisClientPool, Seconds}
import org.apache.spark.sql.{Dataset, ForeachWriter, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import cats.implicits._
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.generic.auto._

import scala.Array

case object DbService {

  def persist1()(stream: DStream[(Ip, EventCount)])(implicit conf: AppConfig, ss: SparkSession, ssc: StreamingContext): Unit = {

    val lookup: config.LookupConfig     = conf.db.lookup
    val registry: config.RegistryConfig = conf.db.registry

    write1(stream, registry.name, registry.ttl)
    write1(filterStream1(stream), lookup.name  , lookup.ttl)
  }

  def persist2()(stream: Dataset[EventCount])(implicit conf: AppConfig, ss: SparkSession, ssc: StreamingContext): Unit = {
    val lookup: config.LookupConfig     = conf.db.lookup
    val registry: config.RegistryConfig = conf.db.registry

    //todo: remove after debug
    stream
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", value = false)
      .option("numRows", 10)
      .queryName("rate-console")
      .start

    stream
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "checkpoint")
      .queryName(registry.name)
      .start("/Users/vnykytenko/IdeaProjects/Spark-Streaming-Final-Project/Streaming-Spark-Consumer/src/main/resources/hdfs/")

    write2(filterStream2(stream), lookup.name, lookup.ttl, lookup.host, lookup.port)

  }

  def filterStream1(stream: DStream[(Ip, EventCount)])(implicit conf: AppConfig): DStream[(Ip, EventCount)] =
    stream.filter { x =>
      val checkEventRate:       EventCount => Int = x => if (x.events > conf.spark.eventRate) 1 else 0
      val checkClickViewRate:   EventCount => Int = x => if (x.views == 0 || x.clicks / x.views > conf.spark.clickViewRate) 1 else 0
      val checkCategoriesRate:  EventCount => Int = x => if (x.category_ids.size > conf.spark.categoriesRate) 1 else 0

      (checkEventRate(x._2) + checkClickViewRate(x._2) + checkCategoriesRate(x._2)) > 0
    }

  def filterStream2(stream: Dataset[EventCount])(implicit conf: AppConfig): Dataset[EventCount] =
    stream.filter { x =>
      val checkEventRate:       EventCount => Int = x => if (x.events > conf.spark.eventRate) 1 else 0
      val checkClickViewRate:   EventCount => Int = x => if (x.views == 0 || x.clicks / x.views > conf.spark.clickViewRate) 1 else 0
      val checkCategoriesRate:  EventCount => Int = x => if (x.category_ids.size > conf.spark.categoriesRate) 1 else 0

      (checkEventRate(x) + checkClickViewRate(x) + checkCategoriesRate(x)) > 0
    }

  private def write1(stream: DStream[(Ip, EventCount)], name: String, ttl: Long) //, offsetRanges: Array[OffsetRange]
                    (implicit ss: SparkSession): Unit = {
    stream
      .map(x => x._2.toString)
      .foreachRDD { rdd =>
       // val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        ss.sparkContext.toRedisSET(rdd, name, ttl.toInt)

     //   stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
  }


  def write2(stream: Dataset[EventCount], name: String, ttl: Long, host: String, port: Int): Unit = {

    object RedisConnection {
      lazy val pool: RedisClientPool = new RedisClientPool(host, port)
    }

    stream
      .writeStream
      .outputMode("append")
      .foreach{
        new ForeachWriter[EventCount] {
          override def open(partitionId: Long, version: Long): Boolean = true
          override def close(errorOrNull: Throwable): Unit = {}
          override def process(value: EventCount): Unit = {
            RedisConnection.pool.withClient {
              client => {
                client.set(s"${name}_${value.ip}", value.toString, onlyIfExists = false, com.redis.Seconds(ttl))
              }
            }
          }
        }
      }
      .queryName(name)
      .start
  }
}

private class RedisSink(host: String, port: Int, ttl: Long) extends ForeachWriter[EventCount] {

  def open(partitionId: Long,version: Long): Boolean = {
    true
  }

  override def process(value: EventCount): Unit = {
    val pool = new RedisClientPool(host, port)
    pool.withClient { client => {
      client.set(s"${value.ip}", value.toString, onlyIfExists = false, Seconds(ttl))
    }}
  }

  def close(errorOrNull: Throwable): Unit = {}
}

