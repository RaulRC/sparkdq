package org.uclm.alarcos.rrc.dq

import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.uclm.alarcos.rrc.CommonTest
import org.uclm.alarcos.rrc.dataquality.completeness.{Interlinking, SchemaCompleteness}
import org.uclm.alarcos.rrc.spark.SparkSpec

/**
  * Created by raul.reguillo on 14/09/17.
  */
@RunWith(classOf[JUnitRunner])
class MeasurementsTest extends CommonTest with SparkSpec with MockFactory {
  //Nodes IDS
  val A = 293150257L
  val B = 293150288L
  val C = 293150319L
  val D = 293150350L
  val E = 293150381L
  val F = 293150412L
  val G = 293150443L

  "Execute getMEasurementSugraph SchemaCompleteness" should "be succesfully" in {
    val testPath = "src/test/resources/dataset/tinysampleSC.nt"
    object MockedTripleReader extends SchemaCompleteness(spark, testPath)
    val step = MockedTripleReader
    val graph = step.loadGraph(spark, testPath)
    val properties = Seq(
      "http://xmlns.com/foaf/0.1/name",
      "http://xmlns.com/foaf/0.1/exprop1"
    )
    val verts = graph.vertices.filter(l => l._2.isURI())
    verts.collect().foreach(println(_))
    val result = step.getMeasurementSubgraph(verts, graph, properties)
    val results = result.collect()
    result.show(1000, truncate=false)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === A & l.get(1) === 1.0) == 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === B & l.get(1) === 1.0) == 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === C & l.get(1) === 1.0) == 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === D & l.get(1) === 0.5) == 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === E & l.get(1) === 0.5) == 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === F & l.get(1) === 0.0) == 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === G & l.get(1) === 0.0) == 1)

  }

  "Execute getMeasurementSubgraph Interlinking Completeness" should "be succesfully" in {
    val testPath = "src/test/resources/dataset/tinysampleI.nt"
    object MockedTripleReader extends Interlinking(spark, testPath)
    val step = MockedTripleReader
    val graph = step.loadGraph(spark, testPath)
    val depth = 4
    val result = step.getMeasurementSubgraph(graph.vertices.filter(l => l._2.isURI()), graph, depth)
    val results = result.collect()
    result.show(1000, truncate=false)
    //    A -> B -> D -> F -> G
    //         | \  |
    //         v  \,v
    //         C -> E


//      +---------+-----+------+------+-------------------+
//      |   source|depth|countT|countF|        measurement|
//      +---------+-----+------+------+-------------------+
//      |293150257|    0|     1|     3|               0.25|
//      |293150257|    1|     3|     3|                0.5|
//      |293150257|    2|     2|     9|0.18181818181818182|
//      |293150257|    3|     1|     6|0.14285714285714285|
//      |293150288|    0|     3|     3|                0.5|
//      |293150288|    1|     2|     9|0.18181818181818182|
//      |293150288|    2|     1|     6|0.14285714285714285|
//      |293150288|    3|     0|     3|                0.0|
//      |293150319|    0|     1|     3|               0.25|
//      |293150319|    1|     0|     3|                0.0|
//      |293150350|    0|     2|     3|                0.4|
//      |293150350|    1|     1|     6|0.14285714285714285|
//      |293150350|    2|     0|     3|                0.0|
//      |293150381|    0|     0|     3|                0.0|
//      |293150412|    0|     1|     3|               0.25|
//      |293150412|    1|     0|     3|                0.0|
//      |293150443|    0|     0|     3|                0.0|
//      +---------+-----+------+------+-------------------+
    //Check some cases in DF
    assert(results.count(l => l.get(0).asInstanceOf[Long] === A & l.get(1) === 0 & l.get(2) === 1 & l.get(3) === 3 & l.get(4) === 0.25) === 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === A & l.get(1) === 1 & l.get(2) === 3 & l.get(3) === 3 & l.get(4) === 0.5) === 1)

    assert(results.count(l => l.get(0).asInstanceOf[Long] === B & l.get(1) === 0 & l.get(2) === 3 & l.get(3) === 3 & l.get(4) === 0.5) === 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === B & l.get(1) === 3 & l.get(2) === 0 & l.get(3) === 3 & l.get(4) === 0.0) === 1)

    assert(results.count(l => l.get(0).asInstanceOf[Long] === F & l.get(1) === 0 & l.get(2) === 1 & l.get(3) === 3 & l.get(4) === 0.25) === 1)
    assert(results.count(l => l.get(0).asInstanceOf[Long] === F & l.get(1) === 1 & l.get(2) === 0 & l.get(3) === 3 & l.get(4) === 0.0) === 1)

    assert(results.count(l => l.get(0).asInstanceOf[Long] === G & l.get(1) === 0 & l.get(2) === 0 & l.get(3) === 3 & l.get(4) === 0.0) === 1)
  }
}
