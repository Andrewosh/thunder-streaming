package org.project.thunder_streaming.rdds

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.SparkContext._


class StreamingTimeSeries(val dstream: DStream[(Int, (Int, Array[Double]))],
                          val interval: Duration = Seconds(3000))
  extends StreamingData[(Int, Array[Double]), StreamingTimeSeries] {

  /** Save streaming data */
  override def save(directory: String, prefix: String): Unit = {
    toStreamingSeries.save(directory, prefix)
  }

  override protected def create(dstream: DStream[(Int, (Int, Array[Double]))]): StreamingTimeSeries = {
    val sts = new StreamingTimeSeries(dstream)
    sts.dstream.checkpoint(interval)
    sts
  }

  /** Print the records (useful for debugging) **/
  override def print(): Unit = {
    toStreamingSeries.print()
  }

  /**
   * Produces a StreamingSeries by first grouping each record by key, then sorting the records based
   * on their time indices, and finally flattening the result.
   *
   * @return a StreamingSeries instance
   */
  lazy val toStreamingSeries: StreamingSeries = {
    val ss = new StreamingSeries(dstream.transform{
      rdd => rdd.groupByKey().map{ case (k, v) => (k, v.toArray.sortBy(_._1).map(_._2).flatten) }
    })
    ss.dstream.checkpoint(interval)
    ss
  }
}

object StreamingTimeSeries {

  /**
    Use the first key in every record of a StreamingSeries as the batch index value in a StreamingTimeSeries
   */
  def fromStreamingSeries(series: StreamingSeries): StreamingTimeSeries = {
    val sts = new StreamingTimeSeries(series.dstream.map{ case (k, v) =>
      if (v.size == 0) {
        (k, (0, Array[Double]()))
      } else {
        (k, (v(0).toInt, v.drop(1)))
      }
    })
    sts.dstream.checkpoint(series.interval)
    sts
  }
}

