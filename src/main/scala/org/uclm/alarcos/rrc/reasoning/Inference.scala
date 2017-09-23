package org.uclm.alarcos.rrc.reasoning

import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Created by Raul Reguillo on 23/09/17.
  */

trait Inference {

  def applyRuleSet(df: DataFrame, graph: Graph[Node, Node], targetColumn: String, newColumn: String, ruleSet: UserDefinedFunction):
  (DataFrame, Graph[Node, Node] )= {
    (df.withColumn(newColumn, ruleSet(col(targetColumn))), graph)
  }
  def applyRuleSet(graph: Graph[Node, Node]): Graph[Node, Node] = {
    graph
  }
}
