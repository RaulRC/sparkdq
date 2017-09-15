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
trait InterlinkingMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurementGlobal(graph: Graph[Node, Node]): Double = {
    val total = graph.vertices.count().toDouble
    (total - graph.vertices.map(vert => vert._2).filter(node => !node.isURI()).count().toDouble)/total
  }

  def getMeasurementSubgraph(subjects: VertexRDD[Node], graph: Graph[Node, Node], depth: Int ): Dataset[Row] = {
    val expanded = expandNodesNLevel(subjects, graph, depth)
    import processSparkSession.implicits._
    val subs = subjects.map(l => (l._1, l._2.getURI())).toDF(Seq("vertexId", "vertexURI"): _*)
    val filteredNodes = graph.vertices.map(l => (l._1, l._2.isURI())).toDF(Seq("nodeId", "isURI"): _*)
    val nodesTF = expanded.join(filteredNodes, $"level" === $"nodeId").drop($"nodeId").drop($"level").orderBy($"source", $"depth")
    val partResultTrue = nodesTF.groupBy($"source", $"depth").agg(count(when($"isURI" === true, true)) as "countT").orderBy($"source", $"depth")
    val partResultFalse = nodesTF.groupBy($"source", $"depth").agg(count(when($"isURI" === false, true)) as "countF").orderBy($"source", $"depth")
      .toDF(Seq("sourceF", "depthF", "countF"): _*)
    val result = partResultTrue.join(partResultFalse, $"source" === $"sourceF" and $"depth" === $"depthF").drop($"sourceF").drop($"depthF").orderBy($"source", $"depth")
      .withColumn("measurement", getRatio($"countT", $"countF")).join(subs, $"source" === $"vertexId").drop($"vertexId")
    result
  }
  def getRatio = udf((totalTrues: Int, totalFalses: Int) => { totalTrues.toDouble/(totalTrues.toDouble + totalFalses.toDouble) })

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

t
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