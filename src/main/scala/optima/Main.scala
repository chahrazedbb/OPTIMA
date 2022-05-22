package optima

import .stopwatch
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

import sys.process._

object Main extends App {
  val stopwatch: StopWatch = new StopWatch
  stopwatch start()
  var queryEngine = "python src/main/python/optima/virtual_model_prediction.py" !! ProcessLogger(stdout append _, stderr append _)
  val mappingsFile = "src/main/evaluation/csv_only/mappings.ttl"//args(1)
  val configFile = "src/main/evaluation/csv_only/config"//args(2)
  val executorID = "local" //jdbc:presto://localhost:8080"//args(3)//"local"
  val reorderJoin = "n"//args(4)
  //val queryEngine = "s"//args(5)
  var queryFile = "src/main/evaluation/queries/query1.sparql" //args(0)

  println("this is the queryEngine")
  queryEngine = queryEngine.split("\\.")(0)
  if (queryEngine == "0") { // Spark as  query engine
    val executor: SparkExecutor = new SparkExecutor(executorID, mappingsFile)
    val run = new Run[DataFrame](executor)
    val (q,e) = run.application(queryFile, mappingsFile, configFile, executorID)
  } else if (queryEngine == "1") { // Spark GraphX as query engine
    val executor: SparkGraphxExecutor = new SparkGraphxExecutor(executorID, mappingsFile)
    val run = new RunGraph[Graph[Array[String], String]](executor)
    val (q,e) = run.application(queryFile, mappingsFile, configFile, executorID)
  }
  stopwatch stop()
  val timeTaken = stopwatch.getTime
  println("this is the time ")
  print(timeTaken)
}
