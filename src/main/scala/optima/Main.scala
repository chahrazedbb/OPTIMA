package optima

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

object Main extends App {

  val mappingsFile = "src/main/evaluation/csv_only/mappings.ttl"//args(1)
  val configFile = "src/main/evaluation/csv_only/config"//args(2)
  val executorID = "local" //jdbc:presto://localhost:8080"//args(3)//"local"
  val reorderJoin = "n"//args(4)
  val queryEngine = "s"//args(5)
  var queryFile = "src/main/evaluation/queries/query1.sparql" //args(0)

    if (queryEngine == "s") { // Spark as  query engine
      val executor: SparkExecutor = new SparkExecutor(executorID, mappingsFile)
      val run = new Run[DataFrame](executor)
      val (q,e) = run.application(queryFile, mappingsFile, configFile, executorID)
    } else if (queryEngine == "g") { // Spark GraphX as query engine
      val executor: SparkGraphxExecutor = new SparkGraphxExecutor(executorID, mappingsFile)
      val run = new RunGraph[Graph[Array[String], String]](executor)
      val (q,e) = run.application(queryFile, mappingsFile, configFile, executorID)
  }
}
