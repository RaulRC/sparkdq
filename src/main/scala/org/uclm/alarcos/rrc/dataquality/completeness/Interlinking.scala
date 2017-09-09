package org.uclm.alarcos.rrc.dataquality.completeness

import org.apache.spark.sql._
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.io.{ReaderRDF}


/**
  * Created by raulreguillo on 6/09/17.
  */
trait InterlinkingMeasurement extends Serializable with ReaderRDF{
  protected val processSparkSession: SparkSession

  def getMeasurement(session: SparkSession, path: String): Unit = {
    val graph = loadGraph(session, path)
    val subjects = getSubjectsWithProperty(graph, "http://xmlns.com/foaf/0.1/Person")
    val expanded = expandNodes(subjects, graph)
    expanded.collect().foreach(println(_))
  }

}

class Interlinking(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends InterlinkingMeasurement{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val result = getMeasurement(sparkSession, inputFile)
  }
}