package optima

object Main extends App {

  val queryFile = args(0)
  val mappingsFile = args(1)
  val configFile = args(2)
  val executorID = args(3)
  val reorderJoin = args(4)
  val queryEngine = args(5)

  if (queryEngine == "s") { // Spark as  query engine
    val executor: SparkExecutor = new SparkExecutor(executorID, mappingsFile)
    val run = new Run[DataFrame](executor)
    val (q, e) = run.application(queryFile, mappingsFile, configFile, executorID)
  } else if (queryEngine == "g") { // Spark GraphX as query engine
    val executor: GraphxExecutor = new GraphxExecutor(executorID, mappingsFile)
    val run = new RunGraph[Graph[Array[String], String]](executor)
    val (q, e) = run.application(queryFile, mappingsFile, configFile, executorID)
  }

}
