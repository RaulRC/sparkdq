package org.uclm.alarcos.rrc.dataquality.completeness

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.uclm.alarcos.rrc.io.ReaderRDF
import org.uclm.alarcos.rrc.models.Measurement
import org.apache.spark.sql.functions._
import org.uclm.alarcos.rrc.reasoning._

/**
  * Created by raulreguillo on 17/09/17.
  */


trait SchemaCompletenessMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurementGlobal(graph: Graph[Node, Node], properties: Seq[String]): Dataset[Row] = {
    import processSparkSession.implicits._
    val edgeRDD = graph.edges.filter(l => true)

    val propIdsRDD = properties.map(p => edgeRDD.filter(l => l.attr.hasURI(p))).reduce(_ union _)
      .map(l => l.srcId).cache()

    val nonPropsDF = edgeRDD.map(l => l.srcId).toDF(Seq("source"): _*)
    nonPropsDF.join(propIdsRDD
      .toDF(Seq("sourceProp"): _*)
      .groupBy($"sourceProp").agg(count($"sourceProp") as "propCount")
      .withColumn("totalProperties", lit(properties.length))
      .withColumn("meas", getRatio($"propCount", $"totalProperties"))
      .drop($"propCount")
      .drop($"totalProperties")
      .drop($"srcId"), $"source" === $"sourceProp", "leftouter")
      .drop($"sourceProp")
        .withColumn("measurement", when(
          col("meas").isNull, 0.0
        ).otherwise(col("meas")))
      .drop($"meas")
      .distinct()
  }
  def getRatio = udf((totalTrues: Int, total: Int) => {
    var res = totalTrues.toDouble/total.toDouble
    if (res > 1.0)
      res = 1.0
    res
  })
  def getMeasurementSubgraph(subjects: VertexRDD[Node], graph: Graph[Node, Node], properties: Seq[String]): Dataset[Row] = {
    import processSparkSession.implicits._
    val subjectsDF = subjects
      .filter(ll=> ll._2 != null)
      .filter(l => l._2.isURI())
      .map(l => (l._1, l._2.toString()))
      .toDF(Seq("srcId", "uri"): _*)
    getMeasurementGlobal(graph, properties).join(subjectsDF, $"source" === $"srcId")
      .drop($"srcId")
  }

  @deprecated
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

class SchemaCompleteness(sparkSession: SparkSession, inputFile: String) extends SchemaCompletenessMeasurement with Inference{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = loadGraph(sparkSession, inputFile)
/*    val properties = Seq(
      "http://www.semanticweb.org/rrc/ontologies/2017/7/semtweet#hasHashtag",
      "http://www.semanticweb.org/rrc/ontologies/2017/7/semtweet#hasUser"
    )*/
    val properties = Seq(
      "http://xmlns.com/foaf/0.1/name",
      "http://dbpedia.org/ontology/birthDate",
      "http://xmlns.com/foaf/0.1/givenName"
    )
    val ant = new Antecedent("measurement", Operator.`<=`, 0.34)
    val cons = new Consequent("contextResult", "GOOD")
    val ruleset = new RuleSet(Seq(Rule(Seq(ant), Conjunction.And, cons)))
    println(graph.edges.count())

    def applyContextualDQ = udf((value: Double) => {
      if (value <= 0.34)
        "BAD"
      else if (value > 0.34 && value <= 0.67)
        "NORMAL"
      else
        "EXCELLENT"
    })

    val (result, newGraph) = applyRuleSet(getMeasurementSubgraph(graph.vertices, graph, properties), graph, "measurement", "contextualResult", applyContextualDQ)
    result.show(10, truncate=false)
//    getMeasurementGlobal(graph, properties).show(1000, truncate=false)
    //getMeasurementSubgraph(graph.vertices, graph, properties).show(1000, truncate=false)
  }
}