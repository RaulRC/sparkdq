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
    val total = graph.vertices.count().toDouble
    (total - graph.vertices.map(vert => vert._2).filter(node => !node.isURI()).count().toDouble)/total
  }

  def getMeasurementSubgraph(subjects: VertexRDD[Node], graph: Graph[Node, Node], depth: Int ): Dataset[Row] = {
    var expanded = expandNodesNLevel(subjects, graph, depth)
    import processSparkSession.implicits._
    val vertDF = graph.vertices.map(l => l._1).toDF(Seq("nodeId"): _*)

    val subjectsDF = vertDF.join(expanded, $"nodeId" === $"source")
    val objectsDF = vertDF.join(expanded, $"nodeId" === $"level")

    val allDF = subjectsDF.union(objectsDF).distinct().drop($"source").drop($"level").drop($"depth")
    val ids = allDF.collect()
    val filteredNodes = graph.vertices.filter(l => ids.contains(l._1.asInstanceOf[Long]))
    val res = filteredNodes.collect()
    val uriNodes = filteredNodes.map(l => (l._1, l._2.isURI())).toDF(Seq("nodeIdF", "isURI"): _*)
    val resultDF = expanded.join(uriNodes, $"level" === $"nodeIdF")

    var measurements: Seq[(Int, Double)] = Seq()
    var leafs: Long = 0
    var tCount: Double = 0
    var pCount: Double = 0

    resultDF
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
      measurements = measurements ++ Seq(Measurement(subjectId, subjectNode, level, (tCount-pCount)/tCount))
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
    //var result = getMeasurementSubject(vertex._1, vertex._2, graph, 3)
    var result = getMeasurementSubgraph(s2, graph, 3)
    result.collect().foreach(println(_))
  }
}