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
trait SchemaCompletenessgMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurementGlobal(graph: Graph[Node, Node]): Double = {
    //ToDo
    null
  }

  def getMeasurementSubgraph(subjects: VertexRDD[Node], graph: Graph[Node, Node], depth: Int ): Dataset[Row] = {
    //ToDo
    null
  }
  def getRatio = udf((totalTrues: Int, totalFalses: Int) => { totalTrues.toDouble/(totalTrues.toDouble + totalFalses.toDouble) })

  def getMeasurementSubject(subjectId: VertexId, subjectNode: Node, graph: Graph[Node, Node], depth: Int ): RDD[Measurement] = {
    //ToDo
    null
  }
}

class Schema(sparkSession: SparkSession, inputFile: String) extends InterlinkingMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val s2 = getSubjectsWithProperty(graph, "http://dbpedia.org/ontology/deathPlace")
    s2.collect().foreach(println(_))
    var result = getMeasurementSubgraph(s2, graph, 3)
    result.show(100000, truncate=false)
    //result.collect().foreach(println(_))
  }
}