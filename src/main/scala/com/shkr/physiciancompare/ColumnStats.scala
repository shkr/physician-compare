package com.shkr.physiciancompare

import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import play.api.libs.json.{ JsValue, Writes, Json }

import scala.util.Try

/**
 * Created by shashank on 3/2/16.
 */
object ColumnStats {

  object CategoricalData {

    @scala.annotation.tailrec
    def mergeWithLimit[T](
      predicate: (T, T) => Boolean,
      leftSide: Seq[T],
      rightSide: Seq[T],
      limit: Int,
      accumulator: Seq[T] = List()
    ): Seq[T] = {
      (leftSide, rightSide) match {
        case exceeded if accumulator.size == limit => accumulator
        case (Nil, _) => accumulator ++ rightSide.take(limit - accumulator.size)
        case (_, Nil) => accumulator ++ leftSide.take(limit - accumulator.size)
        case (l :: ls1, r :: rs1) =>
          if (predicate(l, r)) mergeWithLimit[T](predicate, ls1, rightSide, limit, accumulator :+ l)
          else mergeWithLimit[T](predicate, leftSide, rs1, limit, accumulator :+ r)
      }
    }

    def createCombiner(columnValue: (String, Long)): Seq[(String, Long)] = Seq(columnValue)

    def mergeValue(topLimit: Int)(counter: Seq[(String, Long)], columnValue: (String, Long)): Seq[(String, Long)] = {
      mergeWithLimit[(String, Long)]((l, r) => l._2 > r._2, counter, createCombiner(columnValue), topLimit)
    }

    def mergeCombiners(topLimit: Int)(
      leftCounter: Seq[(String, Long)],
      rightCounter: Seq[(String, Long)]
    ): Seq[(String, Long)] = {
      mergeWithLimit[(String, Long)]((l, r) => l._2 > r._2, leftCounter, rightCounter, topLimit)
    }
  }

  object ContinuousData {

    def createCombiner(columnValue: String): (Double, Long) = Try(columnValue.toDouble).isSuccess match {
      case true => (columnValue.toDouble, 1L)
      case false => (0.0, 0L)
    }

    def mergeValue(counter: (Double, Long), columnValue: String): (Double, Long) =
      Try(columnValue.toDouble).isSuccess match {
        case true => (counter._1 + columnValue.toDouble, counter._2 + 1L)
        case false => counter
      }

    def mergeCombiners(
      leftCounter: (Double, Long),
      rightCounter: (Double, Long)
    ): (Double, Long) =
      (leftCounter._1 + rightCounter._1, leftCounter._2 + rightCounter._2)
  }

  case class Stats(
    column: String,
    topValues: Seq[String],
    topCounts: Seq[Long],
    distinctCount: Long,
    mean: Double
  )

  implicit val writes: Writes[Stats] = new Writes[Stats] {
    def writes(column: Stats) = Json.obj(
      "column" -> column.column,
      "top_values" -> column.topValues,
      "top_counts" -> column.topCounts,
      "distinct_count" -> column.distinctCount,
      "mean" -> column.mean
    )
  }

  def count(input: RDD[(String, String)], k: Int): JsValue = {

    input.cache()

    var distinctValueCount: Accumulator[Long] = input.sparkContext.accumulator[Long](0L)

    val categoricalData: RDD[(String, Int)] =
      input.map(value => (value._1, Set(value._2)))
        .reduceByKey(_ ++ _)
        .map(keyValue => (keyValue._1, keyValue._2.size))

    val categoricalDataCount: RDD[(String, Seq[(String, Long)])] =
      input.map(value => ((value._1, value._2), 1L)).reduceByKey(_ + _)
        .map(keyValue => (keyValue._1._1, (keyValue._1._2, keyValue._2)))
        .combineByKey(
          CategoricalData.createCombiner,
          CategoricalData.mergeValue(k),
          CategoricalData.mergeCombiners(k)
        )

    val continuousDataCount: RDD[(String, (Double, Long))] = input.combineByKey(
      ContinuousData.createCombiner,
      ContinuousData.mergeValue,
      ContinuousData.mergeCombiners
    )

    val columnStats = continuousDataCount.join(categoricalDataCount).join(categoricalData)
      .map(data =>
        Stats(
          data._1,
          data._2._1._2.map(_._1),
          data._2._1._2.map(_._2),
          data._2._2,
          if (data._2._1._1._2 != 0) data._2._1._1._1 / data._2._1._1._2 else 0.0
        )).collect().map(item => (item.column, Json.toJson(item))).toMap

    Json.toJson(columnStats)
  }
}
