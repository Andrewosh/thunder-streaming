package org.project.thunder_streaming.analyses

import breeze.linalg.DenseVector
import org.apache.commons.collections.buffer.CircularFifoBuffer
import org.apache.spark.rdd.RDD

import org.project.thunder_streaming.regression.StatefulBinnedRegression
import org.apache.spark.SparkContext._
import org.project.thunder_streaming.rdds.{StreamingTimeSeries, StreamingSeries}
import org.project.thunder_streaming.regression.{StatefulLinearRegression, StatefulBinnedRegression}
import org.project.thunder_streaming.util.ThunderStreamingContext

import spray.json._
import DefaultJsonProtocol._

import breeze.signal._
import breeze.linalg._
import breeze.numerics._

import collection.JavaConversions._

abstract class SeriesTestAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
  extends Analysis[StreamingSeries](tssc, params) {

  def load(path: String): StreamingSeries = {
    val format = params.getSingleParam(SeriesTestAnalysis.FORMAT_KEY)
    tssc.loadStreamingSeries(path, inputFormat = format)
  }

  override def run(data: StreamingSeries): StreamingSeries = {
    analyze(data)
  }

  def analyze(data: StreamingSeries): StreamingSeries

}

object SeriesTestAnalysis {
  final val DATA_PATH_KEY = "data_path"
  final val FORMAT_KEY = "format"
}

class SeriesMeanAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val mean = data.seriesMean()
    mean
  }
}

class SeriesBatchMeanAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val batchMean = data.dstream.map{ case (k, v) => (k, Array(v.reduce(_ + _) / v.size)) }
    new StreamingSeries(batchMean)
  }
}

class SeriesFiltering1Analysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {

  override def handleUpdate(update: (String, String)): Unit = {
    UpdatableParameters.setUpdatableParam("keySet", update._2)
  }

  def analyze(data: StreamingSeries): StreamingSeries = {
    data.dstream.foreachRDD{ rdd: RDD[(Int, Array[Double])] => {
        val keySet = UpdatableParameters.getUpdatableParam("keySet")
        val newRdd = keySet match {
          case Some(k) => {
            val keys: Set[Int] = JsonParser(k).convertTo[List[Int]].toSet[Int]
            if (!keys.isEmpty) {
              rdd.filter { case (k, v) => keys.contains(k)}
            } else {
              rdd
            }
          }
          case _ => rdd
        }
        println("Collected RDD: %s".format(newRdd.take(20).mkString(",")))
      }
    }
    data
  }
}

class SeriesFiltering2Analysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {

  val partitionSize = params.getSingleParam("partition_size").toInt
  val dims = params.getSingleParam("dims").parseJson.convertTo[List[Int]]

  def getKeysFromJson(keySet: Option[String], dims: List[Int]): List[Set[Int]]= {
    val parsedKeys = keySet match {
        case Some(s) => {
          JsonParser(s).convertTo[List[List[List[Double]]]]
        }
        case _ => List()
    }
    val keys = parsedKeys.map(_.map(key => {
        key.zipWithIndex.foldLeft(0){ case (sum, (dim, idx)) => (sum + (dims(idx) * dim)).toInt }
    }).toSet[Int])
    keys
  }

  override def handleUpdate(update: (String, String)): Unit = {
    UpdatableParameters.setUpdatableParam("keySet", update._2)
  }

  def analyze(data: StreamingSeries): StreamingSeries = {
    val filteredData = data.dstream.transform { rdd =>

      val keySet = UpdatableParameters.getUpdatableParam("keySet")

      val keys = getKeysFromJson(keySet, dims)

      val withIndices = keys.zipWithIndex
      val setSizes = withIndices.foldLeft(Map[Int, Int]()) {
        (curMap, s) => curMap + (s._2 -> s._1.size)
      }

      // Reindex the (k,v) pairs with their set inclusion values as K
      val mappedKeys = rdd.flatMap { case (k, v) =>
        val setMatches = withIndices.map { case (set, i) => if (set.contains(k)) (i, v) else (-1, v)}
        setMatches.filter { case (k, v) => k != -1}
      }

      // For each set, compute the mean time series (pointwise addition divided by set size)
      val sumSeries = mappedKeys.reduceByKey((arr1, arr2) => arr1.zip(arr2).map { case (v1, v2) => v1 + v2})
      val meanSeries = sumSeries.map { case (idx, sumArr) => (idx, sumArr.map(x => x / setSizes(idx)))}

      // Do some temporal averaging on the (spatial) mean time series
      val avgSeries = meanSeries.map{ case (idx, meanArray) => (idx, meanArray.sliding(partitionSize).map(x => x.reduce(_+_) / x.size).toArray[Double]) }
      avgSeries
    }
    new StreamingSeries(filteredData)
  }
}

class SeriesLinearRegressionAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {

  val dims = params.getSingleParam("dims").parseJson.convertTo[List[Int]]
  val numRegressors = params.getSingleParam("num_regressors").parseJson.convertTo[Int]
  val selected = params.getSingleParam("selected").parseJson.convertTo[Set[Int]]

  var regressors: Map[Int, CircularFifoBuffer] = _

  val tau = 3.0
  val tau1 = 1.0
  val windowSize = 50

  val doubleExpFilter: DenseVector[Double] = {
    val x = linspace(0, windowSize, windowSize)
    val ker = DenseVector.vertcat(DenseVector.zeros[Double](windowSize - 1), (exp(-x/tau) - exp(-x/tau1)))
    ker / sum(ker)
  }

  /**
   * Do any preprocessing of the regressors here, and return a StreamingSeries with those keys
   * having been modified.
   */
  def preprocessBehaviors(data: StreamingSeries, keys: Array[Int]): StreamingSeries = {
    val keySet = keys.toSet[Int]
    regressors = keys.map{ k => (k -> new CircularFifoBuffer(Array.fill(windowSize*3)(0.0).toList)) }.toMap

    /*
    // Insert new data into each of the regressor buffers
    data.filterOnKeys(k => keySet.contains(k)).dstream.foreachRDD{
      rdd => rdd.collect().foreach {
        case (k, v) =>  putRegressor(k, v)
      }
    }
    */

    def putRegressor(key: Int, value: Array[Double]) = {
      regressors.get(key) match { case Some(buf) => value.map(buf.add(_)) }
    }

    def getRegressor(key: Int): Array[Double] = {
      regressors(key).toArray.map(x => x.asInstanceOf[Double])
    }

    // Insert the modified regressors into the original data stream before the regression
    data.apply { case (key, value) =>
          // Update the regressors, then return a new StreamSeries with those modified regressors
      if (keySet.contains(key) && (regressors != null)) {
          if (key == keys(0)) {
            val forward = value.map(x => x / 10000.0 - 3.0)
            (key, forward)
          } else if (key == keys(1)) {
            val backward = value.map(x => x / 10000.0 - 3.0)
            (key, backward)
          } else if (key == keys(2)) {
            val state = getRegressor(key)
            val behavior = value.map(x => x / 10000.0 - 1.0)
            val behavVector = DenseVector.vertcat(new DenseVector(state) ,new DenseVector(behavior))
            // Convolve the behavior with the difference of exponentials filter
            val convolved = convolve(behavVector, doubleExpFilter, overhang = OptOverhang.PreserveLength)
                                .toArray.drop(state.size)
            // Store the resulting convolution as the new state
            putRegressor(key, convolved)
            println("convolved: %s".format(convolved.mkString(",")))
            (key, convolved)
          } else {
            (key, value)
          }
      } else {
        (key, value)
      }
    }
  }

  /*
  def getSingleRecordCount(data: StreamingSeries): Int = {
    // Just use the 0 key
    data.filterOnKeys(k => k == 0).dstream.map{ case (k, v) => v.size }.reduce(_+_)
  }
  */

  def analyze(data: StreamingSeries): StreamingSeries = {

    // First, make sure the data is properly ordered in time
    val orderedData = StreamingTimeSeries.fromStreamingSeries(data).toStreamingSeries

    val totalSize = dims.foldLeft(1)(_ * _) + 2
    // For now, assume the regressors are the final numRegressors keys
    val featureKeys = ((totalSize - numRegressors) until totalSize).toArray
    val startIdx = totalSize - numRegressors
    val selectedKeys = featureKeys.zipWithIndex.filter{ case (f, idx) => selected.contains(idx)}.map(_._1)
    println("selectedKeys: %s, featureKeys: %s".format(selectedKeys.mkString(","), featureKeys.mkString(",")))

    // For Nikita's case, convolve the behavioral variables with a difference of exponentials filter
    //val count = getSingleRecordCount(data)
    val preprocessedData = preprocessBehaviors(orderedData, selectedKeys)
    //val preprocessedData = data

    val regressionStream = StatefulLinearRegression.run(preprocessedData, featureKeys, selectedKeys)
    regressionStream.checkpoint(orderedData.interval)
    val outputStream = regressionStream.map{ case (k, model) => (k, (model.normalizedBetas :+ model.r2)) }
    outputStream.map{ case (k,v) => (k, v.mkString(",")) }.print()
    new StreamingSeries(outputStream)
  }
}

class SeriesBinnedRegressionAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
  extends SeriesTestAnalysis(tssc, params) {

  val dims = params.getSingleParam("dims").parseJson.convertTo[Array[Int]]
  val edges = params.getSingleParam("edges").parseJson.convertTo[Array[Double]]
  val numRegressors = params.getSingleParam("num_regressors").parseJson.convertTo[Int]
  val selected = params.getSingleParam("selected").parseJson.convertTo[Int]

  def analyze(data: StreamingSeries): StreamingSeries = {

    val totalSize = dims.foldLeft(1)(_ * _) + 2
    // For now, assume the regressors are the final numRegressors keys
    val featureKeys = ((totalSize - numRegressors) until totalSize).toArray
    val startIdx = totalSize - numRegressors
    val selectedKeys = featureKeys.zipWithIndex.filter{ case (f, idx) => selected == idx }.map(_._1)
    val selectedKey = selectedKeys(0)

    val regressionStream = StatefulBinnedRegression.run(data, selectedKey, edges)
    regressionStream.checkpoint(data.interval)
    val binCenters = StatefulBinnedRegression.binCenters(edges)
    val statsMap = regressionStream.map{ case (int, mixedCounter) =>
      (int, mixedCounter.r2 +: Array[Double](mixedCounter.weightedMean(binCenters)))}
    new StreamingSeries(statsMap)
  }
}

/*
class SeriesFilteringRegressionAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {

  val partitionSize = params.getSingleParam("partition_size").toInt
  val dims = params.getSingleParam("dims").parseJson.convertTo[List[Int]]
  val numRegressors = params.getSingleParam("num_regressors").parseJson.convertTo[Int]


  def getKeysFromJson(keySet: Option[String], existingKeys: Set[Int], dims: List[Int]): List[Set[Int]]= {
    val parsedKeys = keySet match {
        case Some(s) => {
          JsonParser(s).convertTo[List[List[List[Double]]]]
        }
        case _ => List()
    }
    val keys: List[Set[Int]] = parsedKeys.map(_.map(key => {
        key.zipWithIndex.foldLeft(0){ case (sum, (dim, idx)) => (sum + (dims(idx) * dim)).toInt }
    }).toSet[Int])
    existingKeys +: keys
  }

  override def handleUpdate(update: (String, String)): Unit = {
    UpdatableParameters.setUpdatableParam("keySet", update._2)
  }

  def analyze(data: StreamingSeries): StreamingSeries = {

    val totalSize = dims.foldLeft(1)(_ * _)
    // For now, assume the regressors are the final numRegressors keys
    val featureKeys = ((totalSize - numRegressors) to (totalSize - 1)).toArray
    val selectedKeys = featureKeys.take(1)
    val selectedKeySet = selectedKeys.toSet[Int]

    val filteredData = data.dstream.transform { rdd =>

      val keySet = UpdatableParameters.getUpdatableParam("keySet")

      val keys = getKeysFromJson(keySet, selectedKeySet, dims)

      val withIndices = keys.zipWithIndex
      val setSizes = withIndices.foldLeft(Map[Int, Int]()) {
        (curMap, s) => curMap + (s._2 -> s._1.size)
      }

      // Reindex the (k,v) pairs with their set inclusion values as K
      val mappedKeys = rdd.flatMap { case (k, v) =>
        val setMatches = withIndices.map { case (set, i) => if (set.contains(k)) (i, v) else (-1, v)}
        setMatches.filter { case (k, v) => k != -1}
      }

      // For each set, compute the mean time series (pointwise addition divided by set size)
      val sumSeries = mappedKeys.reduceByKey((arr1, arr2) => arr1.zip(arr2).map { case (v1, v2) => v1 + v2})
      val meanSeries = sumSeries.map { case (idx, sumArr) => (idx, sumArr.map(x => x / setSizes(idx)))}

      // Do some temporal averaging on the (spatial) mean time series
      val avgSeries = meanSeries.map{ case (idx, meanArray) => (idx, meanArray.sliding(partitionSize).map(x => x.reduce(_+_) / x.size).toArray[Double]) }
      avgSeries
    }

    println("featureKeys: %s, selectedKeys: %s".format(featureKeys.mkString(","), selectedKeys.mkString(",")))
    StatefulLinearRegression.run(new StreamingSeries(filteredData), featureKeys, selectedKeys)
  }
}
*/

class SeriesNoopAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.dstream.filter{ case (k, v) => k == (512 * 512 * 4 + 2 - 1) }
        .map{ case (k, v) => (k, v.mkString(",")) }.print()
    data
  }
}

class SeriesStatsAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.seriesStats()
  }
}

class SeriesCountingAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val stats = data.seriesStats()
    val counts = stats.applyValues(arr => Array(arr(0)))
    counts
  }
}

class SeriesCombinedAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val means = data.seriesMean()
    val stats = data.seriesStats()
    val secondMeans = data.seriesMean()
    new StreamingSeries(secondMeans.dstream.union(means.dstream.union(stats.dstream)))
  }
}


