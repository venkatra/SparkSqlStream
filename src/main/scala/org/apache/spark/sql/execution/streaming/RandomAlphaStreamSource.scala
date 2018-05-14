package org.apache.spark.sql.execution.streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import javax.annotation.concurrent.GuardedBy
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

/**
  * This case class represents the schema outputted by this stream source. It can be used when u want to convert
  * the stream to a dataset. example:
  * <code>
  * val stream = spark.readStream.format("random_alpha").option("len", 5).load
  * val stream_ds = stream.as[RandomAlphaStreamData]
  * </code>
  *
  * @param timestamp
  * @param value
  */
case class RandomAlphaStreamData(timestamp: Long, value: String)

/**
  * The source that generates a record of format (timestamp ,a random string).The use of this is how to create a basic
  * streaming source with very little functionality (no recovery for example).
  *
  */
object RandomAlphaStreamSource {

  val SCHEMA_TIMESTAMP = StructType(
    StructField("timestamp", TimestampType) ::
      StructField("value", StringType) ::
      Nil)
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}

class RandomAlphaStreamSource(length: Int, sqlContext: SQLContext)
  extends DontCareRecoveryStreamSourceBase[(Timestamp, String)](sqlContext) {

  @GuardedBy("this")
  private var readThread: Thread = null

  /** Returns the schema of the data from this source */
  override def schema = RandomAlphaStreamSource.SCHEMA_TIMESTAMP

  override def create_listBuffer(): ListBuffer[(Timestamp, String)] = {
    new ListBuffer[(Timestamp, String)]
  }

  initialize()

  override def initialize(): Unit = synchronized {

    readThread = new Thread(s"RandomStringGenerator($length)") {
      setDaemon(true)

      override def run(): Unit = {
        while (1 == 1) {
          val newData: (Timestamp, String) = (
            Timestamp.valueOf(
              RandomAlphaStreamSource.DATE_FORMAT.format(Calendar.getInstance().getTime())),
            RandomStringUtils.randomAlphanumeric(length)
          )

          RandomAlphaStreamSource.this.synchronized {
            currentOffset += 1
            batches.append(newData)
          }
          try {
            Thread.sleep(2)
          } catch {
            case e: Exception => //ignore
          }
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

  override def toString: String = s"RandomAlphaStreamSource[lenght: $length]"

  override def stop(): Unit = {}
}

class RandomAlphaStreamSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {


  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    logWarning("This source should not be used for production applications! " +
      "It does not support recovery.")
    if (!parameters.contains("len")) {
      throw new AnalysisException("Set a size of string to generate with option(\"len\").")
    }
    if (schema.nonEmpty) {
      throw new AnalysisException("This source does not support a user-specified schema.")
    }

    val sourceSchema = RandomAlphaStreamSource.SCHEMA_TIMESTAMP

    ("randomAlphaStream", sourceSchema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    val strlength = parameters("len").toInt
    new RandomAlphaStreamSource(strlength, sqlContext)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "random_alpha"
}