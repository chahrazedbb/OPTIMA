package org.squerall

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame
import org.squerall.model.DataQueryFrame

/**
  * Created by mmami on 26.01.17.
  */
object Main extends App {

    var queryFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/queries/Q1.sparql"//args(0)
    val mappingsFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/mappings.ttl"//args(1)
    val configFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/config"//args(2)
    val executorID = "local"//args(3)
    val reorderJoin = "n"//args(4)
    val queryEngine = "g"//args(5)

    if (queryEngine == "s") { // Spark as query engine
        val executor : SparkExecutor = new SparkExecutor(executorID, mappingsFile)
        val run = new Run[DataFrame](executor)
        run.application(queryFile,mappingsFile,configFile,executorID)

    } else if(queryEngine == "p") { // Presto as query engine
        val executor : PrestoExecutor = new PrestoExecutor(executorID, mappingsFile)
        val run = new Run[DataQueryFrame](executor)
        run.application(queryFile,mappingsFile,configFile,executorID)

    } else if(queryEngine == "g") { // Spark GraphX as query engine
        val executor : SparkGraphxExecutor = new SparkGraphxExecutor(executorID, mappingsFile)
        val run = new RunGraph[Graph[Array[String],String]](executor)
        run.application(queryFile,mappingsFile,configFile,executorID)
    }

}
