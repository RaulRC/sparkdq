package org.uclm.alarcos.rrc.reasoning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Created by Raul Reguillo on 23/09/17.
  */

trait Inference {

  def applyRuleSet(df: DataFrame, targetColumn: String, newColumn: String, ruleSet: UserDefinedFunction): DataFrame= {
    val newDF = df.withColumn(newColumn, ruleSet(col(targetColumn)))
    newDF
  }
}
