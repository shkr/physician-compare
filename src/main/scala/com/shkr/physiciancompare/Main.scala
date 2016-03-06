package com.shkr.physiciancompare

import java.nio.file.{ Paths, Files }

import com.shkr.physiciancompare.BipartiteGraph.{ EdgeProperty, VertexProperty }
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

import Configuration._
import play.api.libs.json.Json

/**
 * Created by shashank on 3/1/16.
 */
object Main extends LazyLogging {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def saveRDDAsOneFile(header: Seq[String], rdd: RDD[Seq[String]], filePath: String): Unit = {
    val sc: SparkContext = rdd.sparkContext

    sc.union(sc.makeRDD(Seq(header.mkString(","))), rdd.map(record => record.mkString(",")))
      .repartition(1)
      .saveAsTextFile(filePath)
  }

  def mkGraph(
    header: Seq[String],
    rdd: RDD[Seq[String]],
    graphType: String
  ): Graph[VertexProperty, EdgeProperty] = BipartiteGraph(header, rdd, GroupPracticeGraph)

  def withCleanDataset(sc: SparkContext, column: Option[String]): Unit = {

    val (header, filtered): (Seq[String], RDD[Seq[String]]) = Dataset.clean(sc)

    val distinctDataset = filtered.distinct()

    distinctDataset.cache()

    logger.info(s"Calculating Column Statistics for $SOURCE_FILE")
    val startTime: Long = System.currentTimeMillis()
    Column Statistics
        Files.write(
          Paths.get(COLUMN_STATS_OUTPUT),
          Json.prettyPrint(ColumnStats.count(distinctDataset.flatMap(e => header.zip(e)), COLUMN_STATS_LIMIT)).getBytes()
        )
    logger.info(s"Time taken for column statistics = ${(System.currentTimeMillis() - startTime) / 1000} seconds")
    column match {
      case Some("PAC_ID") => PAC_ID(header, distinctDataset)
      case Some("CCN_1") => CCN(header, distinctDataset)
      case _ => logger.error("You must enter a column label (PAC_ID or CCN_1) as the first argument")
    }
  }

  def PAC_ID(header: Seq[String], distinctDataset: RDD[Seq[String]]): Unit = {

    //Network Stats
    val groupPracticeGraph = mkGraph(header, distinctDataset, "PAC ID")
    var startTime: Long = System.currentTimeMillis()

    logger.info("Calculating PAGE RANK Scores for nodes for the PAC ID & NPI Bipartite Graph")
    Files.write(Paths.get(PAGE_RANK_OUTPUT), Json.prettyPrint(NetworkStats.pageRank(groupPracticeGraph, PAGE_RANK_LIMIT)).getBytes())
    logger.info(s"Time taken for page rank score = ${(System.currentTimeMillis() - startTime) / 1000} seconds")

    startTime = System.currentTimeMillis()
    logger.info("Calculating Connected Components in the PAC ID & NPI Bipartite Graph")
    Files.write(
      Paths.get(CONNECTED_COMPONENTS_OUTPUT),
      Json.prettyPrint(NetworkStats.connectedComponents(groupPracticeGraph, CONNECTED_COMPONENTS_LIMIT)).getBytes()
    )
    logger.info(s"Time taken for connected component = ${(System.currentTimeMillis() - startTime) / 1000} seconds")
  }

  def CCN(header: Seq[String], distinctDataset: RDD[Seq[String]]): Unit = {

    val ccnGraph = mkGraph(header, distinctDataset, "CCN1")
    var startTime: Long = System.currentTimeMillis()

    logger.info("Calculating PAGE RANK Scores for nodes for the PAC ID & NPI Bipartite Graph")
    Files.write(Paths.get(PAGE_RANK_OUTPUT), Json.prettyPrint(NetworkStats.pageRank(ccnGraph, PAGE_RANK_LIMIT)).getBytes())
    logger.info(s"Time taken for page rank score = ${(System.currentTimeMillis() - startTime) / 1000.0} seconds")

    startTime = System.currentTimeMillis()
    logger.info("Calculating Connected Components in the CCN 1 & NPI Bipartite Graph")
    Files.write(
      Paths.get(CONNECTED_COMPONENTS_OUTPUT),
      Json.prettyPrint(NetworkStats.connectedComponents(ccnGraph, CONNECTED_COMPONENTS_LIMIT)).getBytes()
    )
    logger.info(s"Time taken for connected component = ${(System.currentTimeMillis() - startTime) / 1000.0} seconds")
  }

  def main(args: Array[String]): Unit = {

    Dataset.deleteOutput()
    Dataset.maybeDownload()
    val sc: SparkContext = SparkContextFactory.getContext

    withCleanDataset(sc, args.headOption)
  }
}
