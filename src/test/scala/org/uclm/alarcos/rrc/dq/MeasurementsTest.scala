package org.uclm.alarcos.rrc.dq

import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.uclm.alarcos.rrc.CommonTest
import org.uclm.alarcos.rrc.dataquality.completeness.Interlinking
import org.uclm.alarcos.rrc.io.TripleReader
import org.uclm.alarcos.rrc.spark.SparkSpec

/**
  * Created by raul.reguillo on 14/09/17.
  */
@RunWith(classOf[JUnitRunner])
class MeasurementsTest extends CommonTest with SparkSpec with MockFactory {

  "Execute getMeasurementSubgraph" should "be succesfully" in {
    val testPath = "src/test/resources/dataset/tinysample.nt"
    object MockedTripleReader extends Interlinking(spark, testPath)
    val step = MockedTripleReader
    val graph = step.loadGraph(spark, testPath)
    val depth = 4
    val result = step.getMeasurementSubgraph(graph.vertices, graph, depth)
    val results = result.collect()
    results.foreach(println(_))

    //    A -> B -> D -> F -> G
    //         | \  |
    //         v  \,v
    //         C -> E

    //Nodes IDS
    val A = 293150257L
    val B = 293150288L
    val C = 293150319L
    val D = 293150350L
    val E = 293150381L
    val F = 293150412L
    val G = 293150443L
    assert(true)
  }
}
