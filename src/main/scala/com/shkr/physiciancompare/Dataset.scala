package com.shkr.physiciancompare

import com.typesafe.scalalogging.LazyLogging

import sys.process._

import java.io.File
import java.net.URL
import java.nio.file.{ Path, Paths, Files }

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.{ Success, Try }

import Configuration._
/**
 * Created by shashank on 3/1/16.
 */
object Dataset extends LazyLogging {

  def deleteOutput(): Try[Unit] = {
    logger.info(s"Deleting existing output files from path $WORK_DIRECTORY")
    Try {
      FileUtils.deleteDirectory(new File(SAVE_CLEAN_FILE_TO))
      Seq(COLUMN_STATS_OUTPUT, PAGE_RANK_OUTPUT, CONNECTED_COMPONENTS_OUTPUT).foreach(filePath => {
        Files.deleteIfExists(Paths.get(filePath))
      })
    }
  }

  def fileDownloader(url: String, filename: String): Try[Unit] = Try {
    new URL(url) #> new File(filename) !!
  }

  /**
   * Download the data from data.medicare.gov website unless it is already there
   *
   * @return
   */
  def maybeDownload(): Try[Unit] = {

    val workPath: Path = Paths.get(WORK_DIRECTORY)
    if (Files.notExists(workPath)) {
      Files.createDirectory(workPath)
    }

    val file: File = new File(SOURCE_FILE)

    Files.notExists(file.toPath) match {
      case true => {
        logger.info(s"File from path $SOURCE_FILE is missing")
        logger.info(s"Downloading file to path $SOURCE_FILE")
        fileDownloader(SOURCE_URL, SOURCE_FILE)
      }
      case false => Success(Unit)
    }
  }

  /**
   * readDataset
   *
   * @param sc
   * @return
   */
  def read(sc: SparkContext): (Seq[String], RDD[Seq[String]]) = {

    val headerStr: String = sc.textFile(SOURCE_FILE).take(1).head
    val header: Seq[String] = headerStr.split(',').toSeq

    val lines: RDD[Seq[String]] =
      sc.textFile(SOURCE_FILE)
        .filter(line => !line.contentEquals(headerStr))
        .map(line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).toSeq)
        .filter(record => record.size == header.length)

    (header, lines)
  }

  def clean(sc: SparkContext): (Seq[String], RDD[Seq[String]]) = {
    val (header, input) = read(sc)
    val filteredPhysician = Filter.physicianFilter(header, input.distinct())
    Filter.columnFilter(filteredPhysician._1, Configuration.keptKeys, filteredPhysician._2.distinct())
  }
}
