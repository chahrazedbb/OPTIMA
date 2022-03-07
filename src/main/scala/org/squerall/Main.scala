package org.squerall

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame
import org.squerall.model.DataQueryFrame

import java.lang.management.ManagementFactory
import com.sun.management.OperatingSystemMXBean

import java.text.DecimalFormat
import org.apache.commons.io.FileUtils

import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.ListBuffer
import au.com.bytecode.opencsv.CSVWriter

import java.util.List
import java.io.{BufferedWriter, FileWriter}


object Main extends App {

  val mappingsFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/old/mappings.ttl"//args(1)
  val configFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/old/config"//args(2)
  val executorID = "local" //jdbc:presto://localhost:8080"//args(3)//"local"
  val reorderJoin = "n"//args(4)
  val queryEngine = "g"//args(5)
  //var timeTable = new Array[Double](20)
  val mb = 1024*1024
  var Query_Analysis, Relevant_Source_Detection,Query_Execution,memo_usage,cpu, nb_resuts , stars, join, project, filter, orderby, limit = ""
  var query_info, exec_time_info:Array[String] = Array.empty
  val runtime = Runtime.getRuntime

  for(a <- 28 to 28) {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()

    //  var queryFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/queries/Q"+a+".sparql" //args(0)
    var queryFile = "/home/chahrazed/queries/order_by_queries/q"+a+".sparql" //args(0)

    if (queryEngine == "s") { // Spark as  query engine
      val executor: SparkExecutor = new SparkExecutor(executorID, mappingsFile)
      val run = new Run[DataFrame](executor)
      val (q,e) = run.application(queryFile, mappingsFile, configFile, executorID)
      query_info = q
      exec_time_info = e
    } else if (queryEngine == "p") { // Presto as query engine
      val executor: PrestoExecutor = new PrestoExecutor(executorID, mappingsFile)
      val run = new Run[DataQueryFrame](executor)
      run.application(queryFile, mappingsFile, configFile, executorID)
    } else if (queryEngine == "g") { // Spark GraphX as query engine
      val executor: SparkGraphxExecutor = new SparkGraphxExecutor(executorID, mappingsFile)
      val run = new RunGraph[Graph[Array[String], String]](executor)
      val (q,e) = run.application(queryFile, mappingsFile, configFile, executorID)
      query_info = q
      exec_time_info = e
    }
    stopwatch stop()
    val timeTaken = stopwatch.getTime

    val memoRuntime = (runtime.totalMemory - runtime.freeMemory)/mb
   //  Thread.sleep(3000) // wait for 1000 millisecond
    val bean = ManagementFactory.getPlatformMXBean(classOf[OperatingSystemMXBean])
    val formatter = new DecimalFormat("#0.00")
    val memoMfact = FileUtils.byteCountToDisplaySize(ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed)
    val cpuMfact = formatter.format(bean.getSystemCpuLoad)

    nb_resuts = query_info(0)
    stars = query_info(1)
    join = query_info(2)
    project = query_info(3)
    filter = query_info(4)
    orderby = query_info(5)
    limit  = query_info(6)

    Query_Analysis = exec_time_info(0)
    Relevant_Source_Detection = exec_time_info(1)
    Query_Execution = exec_time_info(2)
    memo_usage = exec_time_info(3)
    cpu = exec_time_info(4)

    //val out = new BufferedWriter(new FileWriter("/home/chahrazed/evaluation_results.csv")) //this line will locate the file in the said directory
 //   val out = new FileWriter("/home/chahrazed/evaluation_results_2.csv", true)
//    val writer = new CSVWriter(out) //this creates a csvWriter object for our file
    //val evaluationSchema=Array("query","stars","join","project","filter","orderby","limit","results","Query_engine","Total_Execution_Time","Query_Analysis","Relevant_Source_Detection","Query_Execution","memo_usage_spark","cpu_spark","memo_usage_runtime","memo_usage_mfact","cpu_mfact") // these are the schemas/headings of our csv file
 //   val evaluationResults = Array("q"+a,stars,join,project,filter,orderby,limit,nb_resuts,queryEngine,timeTaken.toString,Query_Execution,Query_Analysis,Relevant_Source_Detection,memo_usage,cpu,memoRuntime.toString,memoMfact,cpuMfact)
    //writer.writeNext(evaluationSchema)// this adds our data into csv file
 //   writer.writeNext(evaluationResults)
 //    out.close() //closing the file
    //timeTable(a) = timeTaken
  }
  //println("the program execution time is " + timeTable.mkString(", "))
/*
*        val map = mutable.LinkedHashMap[String, String](
         "Max mem:" -> FileUtils.byteCountToDisplaySize( ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax),
         "CPU:" -> (formatter.format(bean.getSystemCpuLoad) + "%"),
         "CPU Average:" -> (formatter.format(bean.getSystemLoadAverage)),
         "Threads:" -> ManagementFactory.getThreadMXBean.getThreadCount.toString,
         "Classes:" -> ManagementFactory.getClassLoadingMXBean.getLoadedClassCount.toString)
       val res = map.toArray.map(x => Array(x._1, x._2))
       res.foreach(
         r => println(r.mkString(","))
       )
  //  val cpu_percentatge = cpu * runtime.availableProcessors()
   // println("cpu percentage " + cpu_percentatge)
   * */
}
