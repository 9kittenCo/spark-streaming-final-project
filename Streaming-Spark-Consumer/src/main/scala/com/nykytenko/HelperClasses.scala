package com.nykytenko

import java.sql.Timestamp

import cats.effect.Effect

package object HelperClasses {
  type Ip = String

  trait LogT extends Product with Serializable

  case object EventError extends LogT

  case class LogEvent(unix_time: Timestamp, category_id: Long, ip: Ip, `type`: String) extends LogT {
    def toEventCount: EventCount = {
      val categories = Set(category_id)
      val (isClick, isView) = if (`type`.equals("click")) (1, 0) else (0, 1)
      EventCount(ip, 1, isClick, isView, categories)
    }

    def toEventCountWithTimestamp: EventCountWithTimestamp = {
      val (isClick, isView) = if (`type`.equals("click")) (1, 0) else (0, 1)
      EventCountWithTimestamp(unix_time, ip, 1, isClick, isView, category_id)
    }
  }

  trait EventCountT extends Product with Serializable

  case class EventCount(ip: Ip, events: Long = 0L, clicks: Long = 0L, views: Long = 0L, category_ids: Set[Long] = Set.empty[Long]) extends EventCountT {
    override def toString: String = s"ip: $ip, event rate: $events, clicks: $clicks, views: $views, categories: ${category_ids.size}"
  }

  case class EventCountWithTimestamp(unix_time: Timestamp, ip: Ip, events: Long, clicks: Long, views: Long, category_ids: Long) extends EventCountT{
    override def toString: String = s"ip: $ip, unix_time: $unix_time, event rate: $events, clicks: $clicks, views: $views, categories: $category_ids"
  }

  case class EventRates(events: Int, clicks: Int, categories: Int)

  trait BotDetection extends Product with Serializable {
    def program[F[_]]()(implicit E: Effect[F]): F[Unit]
  }

}
