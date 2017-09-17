package org.uclm.alarcos.rrc.dataquality.completeness

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.uclm.alarcos.rrc.io.ReaderRDF
import org.uclm.alarcos.rrc.models.Measurement
import org.apache.spark.sql.functions._

/**
  * Created by raulreguillo on 6/09/17.
  */

case class SchemaMeasurement(srcId: VertexId, measurement: Boolean, uri: String)

trait SchemaCompletenessgMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurementGlobal(graph: Graph[Node, Node], properties: Seq[String]): Double = {
    graph.edges.filter(l => properties.map(p => l.attr.hasURI(p)).foldLeft(true)(_ && _)).map(ll => ll.srcId).distinct().count().toDouble/
      graph.edges.map(l=>l.srcId).distinct().count().toDouble
  }

  def getMeasurementSubgraph(subjects: VertexRDD[Node], graph: Graph[Node, Node], properties: Seq[String]): Dataset[Row] = {
    import processSparkSession.implicits._

    graph.vertices
      .filter(l => l._2.isURI())
      .map(l => (l._1, l._2.getURI()))
      .toDF(Seq("srcId", "uri"): _*)
      .join(
        graph.edges
          .filter(ll => properties.map(p => ll.attr.hasURI(p)).foldLeft(true)(_ && _))
          .map(line => (line.srcId, true))
          .distinct()
          .toDF(Seq("source", "partialMeasurement"): _*), $"srcId" === $"source", "leftouter")
      .withColumn("measurement", when(col("partialMeasurement").isNotNull, true).otherwise(false))
      .drop($"source")
      .drop($"partialMeasurement")
  }

  def getMeasurementSubject(subjectId: VertexId, graph: Graph[Node, Node], properties: Seq[String]): Boolean = {
    import processSparkSession.implicits._
    if (graph.edges
      .filter(l => l.srcId == subjectId)
      .filter(ll => properties.map(p => ll.attr.hasURI(p)).foldLeft(true)(_ && _))
      .distinct()
      .count().toDouble >= 1
    ) true else false
  }
}

class SchemaCompletenessTF(sparkSession: SparkSession, inputFile: String) extends SchemaCompletenessgMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val result = getMeasurementSubgraph(graph.vertices, graph, Seq(
      "http://dbpedia.org/ontology/birthPlace",
      "http://dbpedia.org/ontology/deathPlace"
    ))
    result.show(1000, truncate=false)
  }
}