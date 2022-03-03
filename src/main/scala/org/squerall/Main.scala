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
  val queryEngine = "s"//args(5)
  //var timeTable = new Array[Double](20)
  val mb = 1024*1024
  var Query_Analysis, Relevant_Source_Detection,Query_Execution,memo_usage,cpu = 0.0
  var nb_resuts = 0.0
  val runtime = Runtime.getRuntime

  for(a <- 10 to 10) {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()

    //  var queryFile = "/home/chahrazed/IdeaProjects/Squeralll/evaluation/input_files/queries/Q"+a+".sparql" //args(0)
    var queryFile = "/home/chahrazed/queries/validating_queries/q"+a+".sparql" //args(0)

    if (queryEngine == "s") { // Spark as  query engine
      val executor: SparkExecutor = new SparkExecutor(executorID, mappingsFile)
      val run = new Run[DataFrame](executor)
      val (nb,q,r,e,m,c) = run.application(queryFile, mappingsFile, configFile, executorID)
      Query_Analysis = q
      Relevant_Source_Detection = r
      Query_Execution = e
      memo_usage = m
      cpu = c
      nb_resuts = nb
    } else if (queryEngine == "p") { // Presto as query engine
      val executor: PrestoExecutor = new PrestoExecutor(executorID, mappingsFile)
      val run = new Run[DataQueryFrame](executor)
      run.application(queryFile, mappingsFile, configFile, executorID)
    } else if (queryEngine == "g") { // Spark GraphX as query engine
      val executor: SparkGraphxExecutor = new SparkGraphxExecutor(executorID, mappingsFile)
      val run = new RunGraph[Graph[Array[String], String]](executor)
      val (nb,q,r,e,m,c) = run.application(queryFile, mappingsFile, configFile, executorID)
      Query_Analysis = q
      Relevant_Source_Detection = r
      Query_Execution = e
      memo_usage = m
      cpu = c
      nb_resuts = nb
    }
    stopwatch stop()
    val timeTaken = stopwatch.getTime

    //val out = new BufferedWriter(new FileWriter("/home/chahrazed/evaluation_results.csv")) //this line will locate the file in the said directory
    //val out = new FileWriter("/home/chahrazed/evaluation_results.csv", true)
    // val writer = new CSVWriter(out) //this creates a csvWriter object for our file
    val evaluationSchema=Array("query","nb_results","Query_engine","Total_Execution_Time","Query_Analysis","Relevant_Source_Detection","Query_Execution","memo_usage","memo_usage_mb","cpu") // these are the schemas/headings of our csv file
    val evaluationResults = Array("q"+a,nb_resuts.toString,queryEngine,timeTaken.toString,Query_Execution.toString,Query_Analysis.toString,Relevant_Source_Detection.toString,(memo_usage).toString,(memo_usage/mb).toString,cpu.toString)
    //writer.writeNext(evaluationSchema)// this adds our data into csv file
    //writer.writeNext(evaluationResults)
    // out.close() //closing the file
    //timeTable(a) = timeTaken

    /*   println("ALL RESULTS IN MB")
       println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory)/mb )
       println("** Free Memory:  " + runtime.freeMemory/mb )
       println("** Total Memory: " + runtime.totalMemory/mb )
       println("** Max Memory:   " + runtime.maxMemory/mb )
 */
    /* Thread.sleep(3000) // wait for 1000 millisecond

     val bean = ManagementFactory.getPlatformMXBean(classOf[OperatingSystemMXBean])
       val formatter = new DecimalFormat("#0.00")
       val map = mutable.LinkedHashMap[String, String](
         "Max mem:" -> FileUtils.byteCountToDisplaySize( ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax),
         "Curr mem:" -> FileUtils.byteCountToDisplaySize(ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed),
         "CPU:" -> (formatter.format(bean.getSystemCpuLoad) + "%"),
         "CPU Average:" -> (formatter.format(bean.getSystemLoadAverage)),
         "Threads:" -> ManagementFactory.getThreadMXBean.getThreadCount.toString,
         "Classes:" -> ManagementFactory.getClassLoadingMXBean.getLoadedClassCount.toString)
       val res = map.toArray.map(x => Array(x._1, x._2))
       res.foreach(
         r => println(r.mkString(","))
       )*/
    val cpu_percentatge = cpu * runtime.availableProcessors()
    println("cpu percentage " + cpu_percentatge)
  }
  //println("the program execution time is " + timeTable.mkString(", "))

}
