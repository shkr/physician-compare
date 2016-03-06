package com.shkr.physiciancompare

import com.shkr.physiciancompare.Configuration.GraphDefinition
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
 * Created by shashank on 3/2/16.
 */
object BipartiteGraph {

  case class VertexProperty(properties: Map[String, String])
  case class EdgeProperty(properties: Map[String, String])

  // The graph might then have the type:
  var graph: Graph[VertexProperty, EdgeProperty] = null

  def apply(
    header: Seq[String],
    input: RDD[Seq[String]],
    graphDefinition: GraphDefinition
  ): Graph[VertexProperty, EdgeProperty] = {

    val vertices: RDD[(VertexId, VertexProperty)] = input.flatMap(record => {

      val properties: Map[String, String] = header.zip(record).toMap

      val leftVertex: Option[(VertexId, VertexProperty)] = Try(
        properties(graphDefinition.LEFT_VERTEX_ID).toLong,
        VertexProperty(graphDefinition.LEFT_PROPERTIES.map(prop => (prop, properties(prop))).toMap +
          (graphDefinition.LEFT_VERTEX_ID -> properties(graphDefinition.LEFT_VERTEX_ID), "label" -> graphDefinition.LEFT_VERTEX_ID))
      ).toOption

      val rightVertex: Option[(VertexId, VertexProperty)] = Try(
        properties(graphDefinition.RIGHT_VERTEX_ID).toLong,
        VertexProperty(graphDefinition.RIGHT_PROPERTIES.map(prop => (prop, properties(prop))).toMap +
          (graphDefinition.RIGHT_VERTEX_ID -> properties(graphDefinition.RIGHT_VERTEX_ID), "label" -> graphDefinition.RIGHT_VERTEX_ID))
      ).toOption

      List(leftVertex, rightVertex).filter(_.isDefined).map(_.get)
    }).distinct()

    val edges: RDD[Edge[EdgeProperty]] = input.flatMap(record => {

      val properties: Map[String, String] = header.zip(record).toMap

      Try {
        val leftVertexLink: VertexId = properties(graphDefinition.LEFT_VERTEX_ID).toLong
        val rightVertexLink: VertexId = properties(graphDefinition.RIGHT_VERTEX_ID).toLong

        val edgeProperty: EdgeProperty = EdgeProperty(graphDefinition.EDGE_PROPERTIES.map(prop => (prop, properties(prop))).toMap)
        Seq(Edge(leftVertexLink, rightVertexLink, edgeProperty), Edge(rightVertexLink, leftVertexLink, edgeProperty))
      }.toOption match {
        case Some(validEdges) => validEdges
        case None => Seq.empty[Edge[EdgeProperty]]
      }
    })

    Graph(vertices, edges)
  }
}