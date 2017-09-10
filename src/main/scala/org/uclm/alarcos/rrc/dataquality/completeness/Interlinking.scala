package org.uclm.alarcos.rrc.dataquality.completeness

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.uclm.alarcos.rrc.io.ReaderRDF
import org.uclm.alarcos.rrc.models.Measurement


/**
  * Created by raulreguillo on 6/09/17.
  */
trait InterlinkingMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurementGlobal(graph: Graph[Node, Node]): Double = {
    graph.vertices.map(vert => vert._2).filter(node => !node.isURI()).count().toDouble/graph.vertices.count().toDouble
  }

  def getMeasurementSubgraph(subjects: VertexRDD[Node], graph: Graph[Node, Node], depth: Int ): RDD[(Int, Double)] = {
    var results: VertexRDD[Node] = subjects
    var measurements: Seq[(Int, Double)] = Seq()
    var leafs: Long = 0
    var tCount: Double = 0
    var pCount: Double = 0
    for (level <- 1 to depth){
      results = expandNodes(results, graph)
      tCount = results.count().toDouble
      pCount = results.filter(line => !line._2.isURI).count().toDouble
      measurements = measurements ++ Seq((level, pCount/tCount))
    }
    processSparkSession.sparkContext.parallelize(measurements)
  }
  def getMeasurementSubject(subjectId: VertexId, subjectNode: Node, graph: Graph[Node, Node], depth: Int ): RDD[Measurement] = {
    var results: VertexRDD[Node] = graph.vertices.filter(line => line._1 == subjectId)
    results.collect().foreach(println(_))
    var measurements: Seq[Measurement] = Seq()
    var tCount: Double = 0
    var pCount: Double = 0
    for (level <- 1 to depth){
      results = expandNodes(results, graph)
      results.collect().foreach(println(_))
      tCount = results.count().toDouble
      pCount = results.filter(line => !line._2.isURI).count().toDouble
      measurements = measurements ++ Seq(Measurement(subjectId, subjectNode, level, pCount/tCount))
    }
    processSparkSession.sparkContext.parallelize(measurements)
  }
}

class Interlinking(sparkSession: SparkSession, inputFile: String) extends InterlinkingMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val s2 = getSubjectsWithProperty(graph, "http://dbpedia.org/ontology/deathPlace")
    s2.collect().foreach(println(_))
    val vertex = s2.take(1)(0)
    var result = getMeasurementSubject(vertex._1, vertex._2, graph, 3)
    result.collect().foreach(println(_))
  }
}