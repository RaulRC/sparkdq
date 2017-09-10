package org.uclm.alarcos.rrc.models

import org.apache.jena.graph.Node
import org.apache.spark.graphx.VertexId

case class Measurement (vertexId: VertexId, node: Node, level: Int, measurement: Double)


