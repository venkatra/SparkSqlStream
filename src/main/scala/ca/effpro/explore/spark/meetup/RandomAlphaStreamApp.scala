package ca.effpro.explore.spark.meetup

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.RandomAlphaStreamData

/**
  * This is where all the functionalities happen.
  */
class RandomAlphaStreamApp {

  val logger = Logger("RandomAlphaStreamApp")

  def MAIN(spark: SparkSession) = {
    val stream = spark.readStream.format("random_alpha").option("len", 5).load

    logger.info(" Stream is on {} ", stream.isStreaming)
    stream.printSchema()

    import spark.implicits._

    val stream_ds = stream.as[RandomAlphaStreamData] // streaming Dataset

    stream_ds.writeStream
      .format("console")
      .start()

    spark.streams.awaitAnyTermination()
  }
}

/**
  * This object class is used for wrapping up the app class, instantiating the spark sessions
  * and invoking the app class.
  */

object RandomAlphaStreamAppMain extends App {

  val spark = SparkSession.builder.appName("RandomAlphaStreamApp").getOrCreate()

  val app = new RandomAlphaStreamApp
  app.MAIN(spark)

  spark.stop()
}