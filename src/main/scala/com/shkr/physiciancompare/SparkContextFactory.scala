package com.shkr.physiciancompare

import org.apache.spark.{ SparkConf, SparkContext }

import Configuration._

/**
 * Created by shashank on 3/1/16.
 */
object SparkContextFactory {

  def getContext: SparkContext = {

    val conf: SparkConf = new SparkConf(true)
      .setAppName(APP_NAME)
      .setMaster(s"local[$NO_OF_CORES]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.logConf", "true")
      .set("spark.eventLog.enabled", "false")

    val sc: SparkContext = new SparkContext(conf)

    //sc.addJar("target/scala-2.11/medicare_physicians_2.11.jar")
    sc
  }
}
