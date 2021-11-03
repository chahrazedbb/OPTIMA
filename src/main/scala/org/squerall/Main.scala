package org.squerall

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame
import org.squerall.model.DataQueryFrame

/**
  * Created by mmami on 26.01.17.
  */
object Main extends App {


    val mappingsFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/mappings.ttl"//args(1)
    val configFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/config"//args(2)
    val executorID = "local" //jdbc:presto://localhost:8080"//args(3)//"local"
    val reorderJoin = "n"//args(4)
    val queryEngine = "g"//args(5)

    var timeTable = new Array[Double](20)

    for( a <- 1 to 2) {
        val stopwatch: StopWatch = new StopWatch
        stopwatch start()

      //  var queryFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/queries/Q"+a+".sparql" //args(0)
      var queryFile = "/home/chahrazed/queries/onlyJoinQueries/fourJoins/q"+a+".sparql" //args(0)

        if (queryEngine == "s") { // Spark as  query engine
            val executor: SparkExecutor = new SparkExecutor(executorID, mappingsFile)
            val run = new Run[DataFrame](executor)
            run.application(queryFile, mappingsFile, configFile, executorID)

        } else if (queryEngine == "p") { // Presto as query engine
            val executor: PrestoExecutor = new PrestoExecutor(executorID, mappingsFile)
            val run = new Run[DataQueryFrame](executor)
            run.application(queryFile, mappingsFile, configFile, executorID)

        } else if (queryEngine == "g") { // Spark GraphX as query engine
            val executor: SparkGraphxExecutor = new SparkGraphxExecutor(executorID, mappingsFile)
            val run = new RunGraph[Graph[Array[String], String]](executor)
            run.application(queryFile, mappingsFile, configFile, executorID)
        } else if (queryEngine == "f") { // Spark GraphX as query engine
            val executor: GraphFrameExecutor = new GraphFrameExecutor(executorID, mappingsFile)
            val run = new Run[GraphFrame](executor)
            run.application(queryFile, mappingsFile, configFile, executorID)
        }

        stopwatch stop()
        val timeTaken = stopwatch.getTime
        timeTable(a) = timeTaken
    }

    println("the program execution time is " + timeTable.mkString(", "))

}
