package org.apache.spark.sql.execution.streaming

import java.io._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import javax.annotation.concurrent.GuardedBy
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

/**
  * A Streaming source implementation of meetup's rsvp stream, as received via HTTP stream. Example:
  * <ol>
  * <li>https://spark.apache.org/docs/2.2.0/streaming-custom-receivers.html</li>
  * <li>https://www.meetup.com/meetup_api/docs/2/rsvps</li>
  * </ol>
  *
  * @param timestamp
  * @param rsvp
  */
case class MeetupRSVPStreamData(timestamp: java.sql.Timestamp, rsvp: String)

/**
  * The implementation of the rsvp stream source. Note this is just a basic implementation of the source and hence is
  * not robust implementations. It does not support functionalities such as recovery ,rollbacks etc. As the messages 
  * are received from the stream, they are added to a memory buffer. The stream then forwards those messages from the 
  * buffer.
  *
  */
object MeetupRSVPStreamSource {

  val SCHEMA_TIMESTAMP = StructType(
    StructField("timestamp", TimestampType) ::
      StructField("rsvp", StringType) ::
      Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}

class MeetupRSVPStreamSource(sqlContext: SQLContext)
  extends DontCareRecoveryStreamSourceBase[(Timestamp, String)](sqlContext) {

  @GuardedBy("this")
  private var readThread: Thread = null

  /** Returns the schema of the data from this source */
  override def schema = MeetupRSVPStreamSource.SCHEMA_TIMESTAMP

  override def create_listBuffer(): ListBuffer[(Timestamp, String)] = {
    new ListBuffer[(Timestamp, String)]
  }

  initialize()

  override def initialize(): Unit = synchronized {

    readThread = new Thread(s"RSVP Stream Receiver") {
      setDaemon(true)

      override def run(): Unit = {
        val httpclient = new HttpClient
        val getMethod = new GetMethod("http://stream.meetup.com/2/rsvps")
        httpclient.executeMethod(getMethod);

        try {
          logInfo("Opening stream ...")
          val bodyStream = getMethod.getResponseBodyAsStream
          val bodyStreamReader = new BufferedReader(new InputStreamReader(getMethod.getResponseBodyAsStream))
          var rsvp = bodyStreamReader.readLine
          while (rsvp != null) {
            val newData: (Timestamp, String) = (
              Timestamp.valueOf(
                MeetupRSVPStreamSource.DATE_FORMAT.format(Calendar.getInstance().getTime())),
              rsvp
            )

            MeetupRSVPStreamSource.this.synchronized {
              currentOffset += 1
              batches.append(newData)
            }
            rsvp = bodyStreamReader.readLine
          }

          logInfo("Closing stream ...")
          bodyStreamReader.close
        } catch {
          case t: Throwable =>
            // restart if there is any other error
            logError("Error receiving data", t)
        }
      }
    }
    readThread.start()
  }

  def toRdd(buff: ListBuffer[(Timestamp, String)]) = {
    sqlContext.sparkContext
      .parallelize(buff)
      .map { case (ts, v) => InternalRow(ts.getTime, UTF8String.fromString(v)) }
  }

  override def toString: String = s"MeetupRSVPStreamSource"

  override def stop(): Unit = {}
}

class MeetupRSVPStreamSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {


  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    logWarning("This source should not be used for production applications! " +
      "It does not support recovery.")
    if (schema.nonEmpty) {
      throw new AnalysisException("This source does not support a user-specified schema.")
    }

    val sourceSchema = MeetupRSVPStreamSource.SCHEMA_TIMESTAMP

    ("MeetupRSVPStream", sourceSchema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    new MeetupRSVPStreamSource(sqlContext)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "meetup_rsvp"
}