# SparkDQ
A Spark extension to support Data Quality Assessment of Semantic Triples

Docs: https://raulrc.github.io/sparkdq/index.html#package


Interlinking assessment for metric: 
```scala
val graph = loadGraph(sparkSession, inputFile)
val depth = 3
getMeasurementSubgraph(graph.vertices, graph, depth),
```

Schema Completeness assessment: 

```scala
val graph = loadGraph(sparkSession, inputFile)
val properties = Seq("Property1", "Property2")
getMeasurementSubgraph(graph.vertices, graph, properties),
```


Contextual Assessment
```scala
    val graph = loadGraph(sparkSession, inputFile)
    val depth = 3
    def setLevels = udf((value: Double)=>{
      if (value <=0.34)
        "BAD"
      else if (value <= 0.67)
        "NORMAL"
      else
        "GOOD"
    })
    
   val result = applyRuleSet(getMeasurementSubgraph(graph.vertices, graph, depth),
      "measurement", "contextualAssessment", setLevels).toDF()
    result.show(10000, truncate = false)
```
