package com.nykytenko

package object HelperClasses {
  type Ip = String

  sealed trait LogT extends Product with Serializable

  case object EventError extends LogT

  case class LogEvent(unix_time: Long, category_id: Int, ip: Ip, `type`: String) extends LogT {
    def toEventCount: EventCount = {
      val categories = Set(category_id)
      val (isClick, isView) = if (`type`.equals("click")) (1, 0) else (0, 1)
      EventCount(ip, 1, isClick, isView, categories)
    }
  }

  case class EventCount(ip: Ip, events: Int, clicks: Int, views: Int, category_ids: Set[Int])

  case class EventRates(events: Int, clicks: Int, categories: Int)

  sealed trait StreamingType

  case object DStreaming extends StreamingType

  case object StructuredStreaming extends StreamingType

}
