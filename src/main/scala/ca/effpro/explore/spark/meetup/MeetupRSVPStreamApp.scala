package ca.effpro.explore.spark.meetup

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MeetupRSVPStreamData

/**
  * This is where all the functionalities happen.
  */
class MeetupRSVPStreamApp {

  val logger = Logger("MeetupRSVPStreamApp")

  def MAIN(spark: SparkSession) = {
    val stream = spark.readStream.format("meetup_rsvp").load

    logger.info(" Stream is on {} ", stream.isStreaming)
    stream.printSchema()

    import spark.implicits._

    val stream_ds = stream.as[MeetupRSVPStreamData] // streaming Dataset

    stream_ds.writeStream
      .format("json")        // can be "orc", "json", "csv", etc.
      .option("path", "target/rsvp_feed")
      .option("checkpointLocation","target/feed_checkpoint")
      .start()

    spark.streams.awaitAnyTermination()
  }
}

/**
  * This object class is used for wrapping up the app class, instantiating the spark sessions
  * and invoking the app class.
  */

object MeetupRSVPStreamAppMain extends App {

  val spark = SparkSession.builder.appName("MeetupRSVPStreamApp").getOrCreate()

  val app = new MeetupRSVPStreamApp
  app.MAIN(spark)

  spark.stop()
}