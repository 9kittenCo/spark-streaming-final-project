package com.nykytenko

import java.sql._

import com.redislabs.provider.redis.toRedisContext
import com.nykytenko.HelperClasses.{EventCount, Ip}
import com.nykytenko.config.{AppConfig, DbConfig}
import com.redis.RedisClientPool
import org.apache.spark.sql.{Dataset, ForeachWriter, SparkSession}
import org.apache.spark.streaming.dstream.DStream

case class DbService(conf: AppConfig, ss: SparkSession) {
  lazy val lookup: config.LookupConfig     = conf.db.lookup
  lazy val registry: config.RegistryConfig = conf.db.registry

  def persist1()(stream: DStream[(Ip, EventCount)]): Unit = {

    write1(botDetect(stream), lookup.name  , lookup.ttl)
    write1(stream           , registry.name, registry.ttl)
  }

  def persist2()(stream: Dataset[EventCount]): Unit = {

    write2(stream                , new RedisSink(registry.host, registry.port, registry.ttl), registry.name)
    write2(stream.filter(isBot _), new RedisSink(lookup.host, lookup.port, lookup.ttl)      , lookup.name)
  }

  def checkEventRate(x: EventCount): Int = if(x.events > conf.spark.eventRate) 1 else 0
  def checkClickViewRate(x: EventCount): Int = if(x.views == 0 || x.clicks / x.views > conf.spark.clickViewRate) 1 else 0
  def checkCategoriesRate(x: EventCount): Int = if(x.category_ids.size > conf.spark.categoriesRate) 1 else 0


  def write1(stream: DStream[(Ip, EventCount)], name: String, ttl: Long): Unit = {
    stream
      .map(x => x._2.toString)
      .foreachRDD { rdd =>
        ss.sparkContext.toRedisSET(rdd, name, ttl.toInt)
      }
  }

  def write2(stream: Dataset[EventCount], writer: RedisSink, name: String): Unit = {
    stream
      .writeStream
      .outputMode("append")
      .foreach(writer)
      .queryName(name)
      .start()
  }

  def isBot(x: EventCount): Boolean = {
    (checkEventRate(x) + checkClickViewRate(x) + checkCategoriesRate(x)) > 0
  }

  def botDetect(stream: DStream[(Ip, EventCount)]): DStream[(Ip, EventCount)] = stream.filter(x => isBot(x._2))

}

class  RedisSink(host: String, port: Int, ttl: Long) extends ForeachWriter[EventCount] {

  def open(partitionId: Long,version: Long): Boolean = {
    true
  }

  override def process(value: EventCount): Unit = {
    val pool = new RedisClientPool(host, port)
    pool.withClient { client => {
      client.set(s"stat_${value.ip}", value.toString, onlyIfExists = false, com.redis.Seconds(ttl))
    }}
  }

  def close(errorOrNull: Throwable): Unit = {}
}

