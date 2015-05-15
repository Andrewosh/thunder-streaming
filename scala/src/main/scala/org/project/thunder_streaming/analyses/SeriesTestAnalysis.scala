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
    // Check if the StreamingSeries has already been loaded. If so, use the existing object. If not,
    // load the series from the path
    SeriesTestAnalysis.load(tssc, path, format)
  }

  override def run(data: StreamingSeries): StreamingSeries = {
    analyze(data)
  }

  def analyze(data: StreamingSeries): StreamingSeries

}

abstract class TimeSeriesTestAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
  extends Analysis[StreamingSeries](tssc, params) {

  def load(path: String): StreamingSeries = {
    val format = params.getSingleParam(SeriesTestAnalysis.FORMAT_KEY)
    // Check if the StreamingSeries has already been loaded. If so, use the existing object. If not,
    // load the series from the path
    TimeSeriesTestAnalysis.load(tssc, path, format)
  }

  override def run(data: StreamingSeries): StreamingSeries = {
    analyze(data)
  }

  def analyze(data: StreamingSeries): StreamingSeries

}

object SeriesTestAnalysis {
  final val DATA_PATH_KEY = "data_path"
  final val FORMAT_KEY = "format"

  var loaded = Map[String, StreamingSeries]()

  /**
    This static load method ensures that all SeriesTestAnalysis classes that load a StreamingSeries
    object from the same path will share the same StreamingSeries in memory (to prevent each Analysis
    from loading their inputs independently).
  **/
  def load(tssc: ThunderStreamingContext, path: String, format: String): StreamingSeries = {
    val maybeExisting = loaded.get(path)
    maybeExisting match {
      case Some(series) => series
      case _ => {
        val series = tssc.loadStreamingSeries(path, inputFormat = format)
        loaded = loaded + (path -> series)
        series
      }
    }
  }

}

object TimeSeriesTestAnalysis {
  final val DATA_PATH_KEY = "data_path"
  final val FORMAT_KEY = "format"

  var loaded = Map[String, StreamingSeries]()

  /**
    This static load method ensures that all SeriesTestAnalysis classes that load a StreamingSeries
    object from the same path will share the same StreamingSeries in memory (to prevent each Analysis
    from loading their inputs independently).
  **/
  def load(tssc: ThunderStreamingContext, path: String, format: String): StreamingSeries = {
    val maybeExisting = loaded.get(path)
    maybeExisting match {
      case Some(series) => series
      case _ => {
        val unorderedSeries = tssc.loadStreamingSeries(path, inputFormat = format)
        val orderedSeries = StreamingTimeSeries.fromStreamingSeries(unorderedSeries).toStreamingSeries
        loaded = loaded + (path -> orderedSeries)
        orderedSeries
      }
    }
  }

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


abstract class SeriesRegressionAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
  extends TimeSeriesTestAnalysis(tssc, params) {

  val numRegressors = params.getSingleParam("num_regressors").toInt
  val selected = params.getSingleParam("selected").parseJson.convertTo[Set[Int]]
  val dims = params.getSingleParam("dims").parseJson.convertTo[List[Int]]

  var featureKeys: Array[Int] = _
  var selectedKeys: Array[Int] = _

  def analyze(data: StreamingSeries): StreamingSeries = {
    val totalSize = dims.foldLeft(1)(_ * _) + numRegressors
    // For now, assume the regressors are the final numRegressors keys
    featureKeys = ((totalSize - numRegressors) until totalSize).toArray
    selectedKeys = featureKeys.zipWithIndex.filter { case (f, idx) => selected.contains(idx) }.map(_._1)
    println("featureKeys: %s, selectedKeys: %s".format(featureKeys.mkString(","), selectedKeys.mkString(",")))
    data
  }

}

/**
 * TODO This needs to not be like this
 */
trait NikitaPreprocessing {

  val tau = 3.0
  val tau1 = 1.0
  val windowSize = 50

  val doubleExpFilter: DenseVector[Double] = {
    val x = linspace(0, windowSize, windowSize)
    val ker = DenseVector.vertcat(new DenseVector(Array.fill(windowSize - 1)(0.0)), exp(-x/tau) - exp(-x/tau1))
    ker / sum(ker)
  }

  /**
   * Do any preprocessing of the regressors here, and return a StreamingSeries with those keys
   * having been modified.
   */
  def preprocessBehaviors(data: StreamingSeries, keys: Array[Int]): StreamingSeries = {
    val keySet = keys.toSet[Int]

    var regressors = keys.map{ k => (k -> Array[Double]()) }.toMap

    def getRegressor(key: Int): Array[Double] = {
      regressors(key)
    }

    data.dstream.filter{case (k, _) => keySet.contains(k)}.foreachRDD{rdd =>
      val batchRegressors = rdd.collect().map{ case (key, value) =>
       val updatedState = regressors.get(key) match {
         case Some(arr) => {
           val appended = value ++: arr
           (key -> appended.take(min(windowSize, appended.size)))
         }
       }
      updatedState}.toMap
      if (batchRegressors.size == keys.size) {
        regressors = batchRegressors
      }
    }

    // Insert the modified regressors into the original data stream before the regression
    data.apply { case (key, value) =>
          // Update the regressors, then return a new StreamSeries with those modified regressors
      if (keySet.contains(key) && (regressors != null)) {
        if (key == keys(0)) {
          val forward = value.map(x => x / 10000.0 - 3.0)
          println("Forward, %s -> %s".format(value.mkString(","), forward.mkString(",")))
          (key, forward)
        } else if (key == keys(1)) {
          val backward = value.map(x => x / 10000.0 - 3.0)
          println("Backward, %s -> %s".format(value.mkString(","), backward.mkString(",")))
          (key, backward)
        } else if (key == keys(2)) {
          val behavior = getRegressor(key).map(x => x / 10000.0 - 1.0)
          println("behavior: %s".format(behavior.mkString(",")))
          val behavVector = new DenseVector[Double](behavior)
          // Convolve the behavior with the difference of exponentials filter
          val convolved = convolve(behavVector, doubleExpFilter, overhang = OptOverhang.PreserveLength).toArray
          val withoutState = convolved.drop(behavior.size - value.size)
          // Store the resulting convolution as the new state
          println("Behavior, %s -> %s".format(behavior.mkString(","), convolved.mkString(",")))
          println("withoutState: %s".format(withoutState.mkString(",")))
          (key, withoutState)
        } else {
          (key, value)
        }
      } else {
        (key, value)
      }
    }
  }
}


class SeriesExtractRegressorsAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesRegressionAnalysis(tssc, params) with NikitaPreprocessing {

  override def analyze(data: StreamingSeries): StreamingSeries = {
    super.analyze(data)

    val preprocessedData = preprocessBehaviors(data, selectedKeys)

    val keySet = featureKeys.toSet[Int]
    preprocessedData.filterOnKeys(keySet.contains(_))
  }
}


class SeriesLinearRegressionAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesRegressionAnalysis(tssc, params) with NikitaPreprocessing {

  override def analyze(data: StreamingSeries): StreamingSeries = {
    super.analyze(data)

    // For Nikita's case, convolve the behavioral variables with a difference of exponentials filter
    //val count = getSingleRecordCount(data)
    val preprocessedData = preprocessBehaviors(data, selectedKeys)
    //val preprocessedData = data

    val regressionStream = StatefulLinearRegression.run(preprocessedData, featureKeys, selectedKeys)
    regressionStream.checkpoint(data.interval)
    val outputStream = regressionStream.map{ case (k, model) => (k, (model.r2 +: model.weights)) }
    outputStream.map{ case (k,v) => (k, v.mkString(",")) }.print()
    new StreamingSeries(outputStream)
  }
}


class SeriesBinnedRegressionAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
  extends SeriesRegressionAnalysis(tssc, params) {

  val edges = params.getSingleParam("edges").parseJson.convertTo[Array[Double]]

  override def analyze(data: StreamingSeries): StreamingSeries = {
    super.analyze(data)

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


