package org.uclm.alarcos.rrc.dataquality.completeness

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.uclm.alarcos.rrc.io.ReaderRDF
import org.uclm.alarcos.rrc.models.Measurement
import org.apache.spark.sql.functions._

/**
  * Created by raulreguillo on 17/09/17.
  */

case class SchemaMeasurement(srcId: VertexId, measurement: Boolean, uri: String)

trait SchemaCompletenessgMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurementGlobal(graph: Graph[Node, Node], properties: Seq[String]): Dataset[Row] = {
    import processSparkSession.implicits._
    val edgeRDD = graph.edges.filter(l => true)
    properties.map(p => edgeRDD.filter(l => l.attr.hasURI(p))).reduce(_ union _)
      .map(l => l.srcId).toDF(Seq("source"): _*)
      .groupBy($"source").agg(count($"source") as "propCount")
      .withColumn("totalProperties", lit(properties.length))
      .withColumn("measurement", getRatio($"propCount", $"totalProperties"))
      .drop($"propCount")
      .drop($"totalProperties")
  }
  def getRatio = udf((totalTrues: Int, total: Int) => {
    var res = totalTrues.toDouble/total.toDouble
    if (res > 1.0)
      res = 1.0
    res
  })
  def getMeasurementSubgraph(subjects: VertexRDD[Node], graph: Graph[Node, Node], properties: Seq[String]): Dataset[Row] = {
    import processSparkSession.implicits._
    val subjectsDF = subjects.filter(l => l._2.isURI()).map(l => (l._1, l._2.getURI())).toDF(Seq("srcId", "uri"): _*)
    getMeasurementGlobal(graph, properties).join(subjectsDF, $"source" === $"srcId")
      .drop($"srcId")
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

class SchemaCompleteness(sparkSession: SparkSession, inputFile: String) extends SchemaCompletenessgMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val properties = Seq(
      "http://dbpedia.org/ontology/birthPlace",
      "http://dbpedia.org/ontology/deathPlace"
    )
/*    val result = getMeasurementSubgraph(graph.vertices, graph, properties)
    result.show(1000, truncate=false)*/
//    getMeasurementGlobal(graph, properties).show(1000, truncate=false)
    getMeasurementSubgraph(graph.vertices, graph, properties).show(1000, truncate=false)
  }
}