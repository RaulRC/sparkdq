package org.uclm.alarcos.rrc.reasoning

/**
  * Created by Raul Reguillo on 23/09/17.
  */

object Conjunction extends Enumeration {
  val And, Or = Value
}

object Operator extends Enumeration {
  val `>`, `<`, `>=`, `<=`, `==` = Value
}

case class Antecedent(columnName: String, operator: Operator.Value, value: Double)
case class Consequent(newColumnName: String, value: Any)

case class Rule(ant: Seq[Antecedent], conjuntion: Conjunction.Value, cons: Consequent)
case class RuleSet(rules: Seq[Rule])

