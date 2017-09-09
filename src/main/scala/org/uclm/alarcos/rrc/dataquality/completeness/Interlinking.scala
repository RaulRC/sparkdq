package org.uclm.alarcos.rrc.dataquality.completeness

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.uclm.alarcos.rrc.io.ReaderRDF


/**
  * Created by raulreguillo on 6/09/17.
  */
trait InterlinkingMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurementGlobal(subjects: VertexRDD[Node], graph: Graph[Node, Node], depth: Int ): RDD[(Int, Double)] = {
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
}

class Interlinking(sparkSession: SparkSession, inputFile: String) extends InterlinkingMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
    val subjects = getSubjectsWithProperty(graph, "http://xmlns.com/foaf/0.1/Person")
    val result = getMeasurementGlobal(subjects, graph, 3)
    result.collect().foreach(println(_))
  }
}