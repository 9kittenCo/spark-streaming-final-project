package com.nykytenko


import cats.effect.{Effect, IO}
import cats.implicits._
import com.nykytenko.HelperClasses.{DStreaming, StreamingType, StructuredStreaming}
import org.slf4j.{Logger, LoggerFactory}

object Main {

  val logger: Logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    val mST   = Map[String, StreamingType](
      "dstream" -> DStreaming,
              "sstream" -> StructuredStreaming
              )

    val name  = args(0).toLowerCase.trim

    if (args.length != 1) logger.error("Wrong number of parameters!")
    else if (mST.isDefinedAt(name)) {
      val stype: StreamingType = mST(name)

      program[IO](stype).unsafeRunSync()
    } else logger.error("Wrong streaming type")
  }

  def program[F[_]](stype: StreamingType)(implicit E: Effect[F]): F[Unit] = {
    for {
      logic <- mainLogic[F](stype)
      _ <- logic.value.process[F]
      _ <- SparkConfigService[F].close(logic.ss, logic.ssc)
    } yield ()
  }

  def mainLogic[F[_]](stype: StreamingType)(implicit E: Effect[F]): F[EtlResult] = {
    for {
      appConf   <- config.load[F]
      spark     <- SparkConfigService[F].createFromConfig(appConf.spark)
    } yield EtlResult(EtlService.getEtl(stype, appConf, spark._2, spark._1), spark._1, spark._2)
  }
}