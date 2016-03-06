package com.shkr.physiciancompare

import java.nio.file.{ Paths, Files }

import com.shkr.physiciancompare.BipartiteGraph.{ EdgeProperty, VertexProperty }
import com.shkr.physiciancompare.Configuration._
import org.apache.spark.graphx._
import play.api.libs.json.{ JsValue, Json }

/**
 * Created by shashank on 3/2/16.
 */
object NetworkStats {

  /**
   * PageRank
   *
   * @param graph
   * @param pageRankLimit
   * @return
   */
  def pageRank(
    graph: Graph[VertexProperty, EdgeProperty],
    pageRankLimit: Int,
    limit: Int = Configuration.COLUMN_STATS_LIMIT
  ): JsValue = {

    val vertexWithPageRank: Map[VertexId, Double] = graph.pageRank(Configuration.PAGE_RANK_TOLERANCE).vertices.top(pageRankLimit)(
      new Ordering[(VertexId, Double)]() {
        override def compare(x: (VertexId, Double), y: (VertexId, Double)): Int = Ordering[Double].compare(x._2, y._2)
      }
    ).toMap

    vertexWithPageRank.map(topVertex => {
      Json.obj(topVertex._1.toString ->
        Json.obj("vertex_id" -> topVertex._1.toString, "page_rank" -> Json.toJson(topVertex._2)).+(
          "properties" -> ColumnStats.count(
            graph.vertices.filter(v => topVertex._1 == v._1).distinct()
              .flatMap(v => v._2.properties.toSeq), Configuration.COLUMN_STATS_LIMIT
          )
        ))
    }).reduce((a, b) => a.++(b))
  }

  /**
   * connectedComponents
   *
   * @param graph
   * @param clusterLimit
   * @return
   */
  def connectedComponents(
    graph: Graph[VertexProperty, EdgeProperty],
    clusterLimit: Int,
    limit: Int = Configuration.COLUMN_STATS_LIMIT
  ): JsValue = {

    val connectedComponents: Map[Long, Iterable[Long]] = graph.connectedComponents().vertices
      .map(v => (v._2, v._1)).groupByKey
      .top(clusterLimit)(
        new Ordering[(VertexId, Iterable[Long])]() {
          override def compare(x: (VertexId, Iterable[Long]), y: (VertexId, Iterable[Long])): Int =
            Ordering[Int].compare(x._2.size, y._2.size)
        }
      ).toMap

    val connectedComponentsProperties: Iterable[JsValue] = connectedComponents.map(cluster => {
      val clusterMembers: List[Long] = cluster._2.toList
      Json.toJson(
        Map(
          "cluster_id" -> Json.toJson(cluster._1),
          "cluster_size" -> Json.toJson(clusterMembers.size),
          "column_stats" -> ColumnStats.count({
            graph.vertices.filter(v => clusterMembers.contains(v._1.toLong))
              .flatMap(v => v._2.properties + ("vertex_id" -> v._1.toString))
          }, limit)
        )
      )
    })

    Json.toJson(
      connectedComponentsProperties ++
        Iterable(Json.obj(
          "clusters_by_size" -> Json.toJson(connectedComponents.map(kV => kV._1.toString -> Json.toJson(kV._2.size)))
        ))
    )
  }
}
