package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable.ListBuffer

/**
  * This class acts as a base classes for those stream implementation which does not care to have a recovery mechanism
  * ,due to datasource limitation. Hence a non-replayable stream source.
  *
  * @param sqlContext
  */
abstract class DontCareRecoveryStreamSourceBase[T](sqlContext: SQLContext)
  extends Source with Logging {

  /**
    * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
    * Stored in a ListBuffer to facilitate removing committed batches.
    */
  protected lazy val batches: ListBuffer[T] = create_listBuffer

  protected var currentOffset: LongOffset = LongOffset(-1L)

  protected var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  def create_listBuffer(): ListBuffer[T]

  /**
    * This method should implement the necessary logic to connect to the stream source and should populate the `batches`
    * . Method should increment the `currentOffset` every time a message is added to the batches.
    */
  def initialize(): Unit


  def toRdd(buff: ListBuffer[T]): RDD[InternalRow]

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val startOrdinal =
      start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      logInfo(s"Returning data for offset (${sliceStart} - ${sliceEnd}]")
      batches.slice(sliceStart, sliceEnd)
    }

    val rdd: RDD[InternalRow] = toRdd(rawList)
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def getOffset: Option[Offset] = synchronized {
    if (currentOffset.offset == -1) {
      None
    } else {
      Some(currentOffset)
    }
  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

}
