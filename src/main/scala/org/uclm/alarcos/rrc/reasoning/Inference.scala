package org.uclm.alarcos.rrc.reasoning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Created by Raul Reguillo on 23/09/17.
  */

trait Inference {
  /**
    *
    * @param df Target dataframe
    * @param targetColumn Target column inside dataframe
    * @param newColumn New column name with the conversion
    * @param ruleSet User Defined Function to apply in order to get a reasoning
    * @return Transformed dataframe
    */
  def applyRuleSet(df: DataFrame, targetColumn: String, newColumn: String, ruleSet: UserDefinedFunction): DataFrame= {
    val newDF = df.withColumn(newColumn, ruleSet(col(targetColumn)))
    newDF
  }
}
