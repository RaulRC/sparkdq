package org.uclm.alarcos.rrc.reasoning

import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

/**
  * Created by Raul Reguillo on 23/09/17.
  */

trait Inference {

  def applyRuleSet(df: DataFrame, ruleSet: RuleSet): DataFrame = {
    df
  }
  def applyRuleSet(graph: Graph[Node, Node]): Graph[Node, Node] = {
    graph
  }
}
