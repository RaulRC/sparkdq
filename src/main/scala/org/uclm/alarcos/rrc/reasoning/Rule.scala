package org.uclm.alarcos.rrc.reasoning

/**
  * Created by Raul Reguillo on 23/09/17.
  */

case class Antecedent(columnName: String, operator: String, value: Double)
case class Consequent(newColumnName: String, value: Any)

case class Rule(ant: Antecedent, cons: Consequent)

