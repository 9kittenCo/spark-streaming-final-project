package com.nykytenko


import cats.effect.IO
import com.nykytenko.HelperClasses.BotDetection
import org.slf4j.{Logger, LoggerFactory}

object Main {

  val logger: Logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    val mST: Map[String, BotDetection] = Map[String, BotDetection](
      "dstream" -> BotDetectionV1,
              "sstream" -> BotDetectionV2
              )
    val name = args.headOption.getOrElse("dstream").toString.toLowerCase.trim

    if (args.length != 1) logger.error("Wrong number of parameters!")
    else if (mST.isDefinedAt(name)) {
      val stype = mST(name)
      stype.program[IO]().unsafeRunSync()
    } else logger.error("Wrong streaming type")
  }
}
