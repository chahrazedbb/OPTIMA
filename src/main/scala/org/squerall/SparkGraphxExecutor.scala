package org.squerall

import java.util
import com.google.common.collect.ArrayListMultimap
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.squerall.Helpers._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SparkGraphxExecutor (sparkURI: String, mappingsFile: String) extends QueryExecutorGraph[Graph[Array[String],String]] {

  def query(sources: mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])],
            optionsMap_entity: mutable.HashMap[String, (Map[String, String], String)],
            toJoinWith: Boolean,
            star: String,
            prefixes: Map[String, String],
            select: util.List[String],
            star_predicate_var: mutable.HashMap[(String, String), String],
            neededPredicates: mutable.Set[String],
            filters: ArrayListMultimap[String, (String, String)],
            leftJoinTransformations: (String, Array[String]),
            rightJoinTransformations: Array[String],
            joinPairs: Map[(String, String), String], edgeId: Int):
  (Graph[Array[String],String],Integer,String,Map[String,Array[String]],Any)  = {

    val spark = SparkSession.builder.master(sparkURI).appName("Squerall").getOrCreate
    val sc = spark.sparkContext
    var edgeIdMap : Map[String, Array[String]] = Map.empty
    var dataSource_count = 0
    var parSetId = ""
    var vertex: RDD[(VertexId, Array[String])] = null
    var finalVer: RDD[(VertexId, Array[String])] = null
    var myheader: Array[String] = Array.empty
    var header = ""
    var myheaderIndex : Array[Int] = Array.empty
    for (s <- sources) {

      dataSource_count += 1 // in case of multiple relevant data sources to union

      val attr_predicate = s._1
      val sourcePath = s._2
      val sourceType = getTypeFromURI(s._3)
      val options = optionsMap_entity(sourcePath)._1 // entity is not needed here in SparkExecutor

      var columns = getSelectColumnsFromSet(attr_predicate, omitQuestionMark(star), prefixes, select, star_predicate_var, neededPredicates, filters)

      val str = omitQuestionMark(star)

      if (select.contains(str)) {
        parSetId = getID(sourcePath, mappingsFile)
        columns = s"$parSetId AS `$str`, " + columns
      }

      if (toJoinWith) { // That kind of table that is the 1st or 2nd operand of a join operation
        val id = getID(sourcePath, mappingsFile)
        if (columns == "") {
          columns = id + " AS " + str + "_ID"
        } else
          columns = columns + "," + id + " AS " + str + "_ID"
      }

      sourceType match {
        case "csv" =>
          val stopwatch: StopWatch = new StopWatch
          stopwatch start()

          val data = sc.textFile(sourcePath)
          //getting the header
          header = data.first()

          //getting column names
          val mycolumns = columns.split(",")
          val myheader2 = header.split(",")
          val toRemove = "`".toSet
          mycolumns.foreach(col =>
            if(myheader2.contains(col.split("AS")(0).filterNot(toRemove).trim)) {
              val  i = myheader2.indexOf(col.split("AS")(0).filterNot(toRemove).trim)
              myheader = myheader :+ col.split("AS")(1).filterNot(toRemove).trim
              myheaderIndex = myheaderIndex :+ i
            }
          )
          //getting data
          var vertex2 =  data.filter(line => line != header)
            .map(line =>  line.split(","))
            .map( parts => ((edgeId+"00"+parts.head).toLong, parts))

           vertex = vertex2.map(v=>(v._1,myheaderIndex.map({i=>v._2(i)})))

          edgeIdMap = Map(str -> Array(edgeId.toString,myheader.mkString(",")))

          stopwatch stop()
          val timeTaken = stopwatch.getTime
          println(s"++++++ loading time : $timeTaken")
        case _ =>
      }
      if(finalVer == null) {
        finalVer = vertex
      } else{
        finalVer = finalVer.union(vertex)
      }
    }

    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    var whereString = ""

    var nbrOfFiltersOfThisStar = 0

    val it = filters.keySet().iterator()
    while (it.hasNext) {
      val value = it.next()
      val predicate = star_predicate_var.
        filter(t => t._2 == value).
        keys. // To obtain (star, predicate) pairs having as value the FILTER'ed value
        filter(t => t._1 == star).
        map(f => f._2).toList

      if (predicate.nonEmpty) {
        val ns_p = get_NS_predicate(predicate.head) // Head because only one value is expected to be attached to the same star an same (object) variable
        val column = omitQuestionMark(star) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)

        nbrOfFiltersOfThisStar = filters.get(value).size()

        val conditions = filters.get(value).iterator()
        while (conditions.hasNext) {
          val operand_value = conditions.next()
          whereString = column + operand_value._1 + operand_value._2
          //getting the column index in the table
          val colIndex = myheader.indexOf(column)

          if (operand_value._1 != "regex") {
            if(isAllDigits(operand_value._2)  && operand_value._1.equals("=")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toDouble==operand_value._2.toDouble)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toDouble<operand_value._2.toDouble)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toDouble>operand_value._2.toDouble)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">=")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toDouble>=operand_value._2.toDouble)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<=")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toDouble<=operand_value._2.toDouble)
                case _ => false
              }
            }else{
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).equals(operand_value._2.split("\"")(1)))
                case _ => false
              }
            }
          }
          // else  finalGP = finalGP.filter(finalGP(column).like(operand_value._2.replace("\"","")))
          // regular expression with _ matching an arbitrary character and % matching an arbitrary sequence
        }
      }
    }

    stopwatch stop()
    val timeTaken = stopwatch.getTime

    //creating temp edges
    val edge: RDD[Edge[String]] = finalVer.map{(v) => Edge(v._1,v._1, "id")}
    //creating graphs
    var graph:Graph[Array[String],String] = Graph(finalVer,edge)

    (graph, nbrOfFiltersOfThisStar, parSetId, edgeIdMap, sc)
  }

  def transform(ps: Any, column: String, transformationsArray: Array[String]): Any = {
    ps.asInstanceOf[Graph[String,String]]
  }

  def join(joins: ArrayListMultimap[String, (String, String)],
           prefixes: Map[String, String],
           star_df: Map[String, Graph[Array[String],String]],
           edgeIdMap: Map[String,Array[String]],
           sc: Any)
  :Graph[Array[String],String] = {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()

    import scala.collection.JavaConversions._
    import scala.collection.mutable.ListBuffer

    var pendingJoins = mutable.Queue[(String, (String, String))]()
    val seenDF : ListBuffer[(String,String)] = ListBuffer()
    var firstTime = true
    var jGrah :Graph[Array[String],String] = null

    val it = joins.entries.iterator
    while ({it.hasNext}) {
      val entry = it.next

      val op1 = entry.getKey
      val op2 = entry.getValue._1
      val jVal = entry.getValue._2

      val vertex1 = star_df(op1).vertices
      val vertex2 = star_df(op2).vertices

      val njVal = get_NS_predicate(jVal)
      val ns = prefixes(njVal._1)

      var id1 : String = ""
      var id2 : String = ""
      var header1 : String = ""
      var header2 : String = ""

      it.remove()

      //getting the added number to the edges ids
      if (edgeIdMap.keySet.contains(omitQuestionMark(op2)) ){
        id2 = edgeIdMap(omitQuestionMark(op2))(0)
        header2 = edgeIdMap(omitQuestionMark(op2))(1)
      }

      if (edgeIdMap.keySet.contains(omitQuestionMark(op1)) ){
        id1 = edgeIdMap(omitQuestionMark(op1))(0)
        header1 = edgeIdMap(omitQuestionMark(op1))(1)
      }

      if (firstTime) {
        firstTime = false
        seenDF.add((op1, jVal))
        seenDF.add((op2, "ID"))
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //getting the fk column
        val colIndex = header1.split(",").indexOf(fk)
        //creating the edges
        val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk) }
        //creating the graph
        jGrah = Graph(vertex1.union(vertex2).filter((v)=>v._2!=null)
          , edges)
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
      } else {
        val dfs_only = seenDF.map(_._1)

        if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //getting the fk column
          val colIndex = header1.split(",").indexOf(fk)
          //creating the edges
          val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk) }
          //creating the graph
          jGrah = Graph(jGrah.vertices.union(vertex2).filter((v)=>v._2!=null)
            , jGrah.edges.union(edges))
          jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
          seenDF.add((op2,"ID"))

        } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //getting the fk column
          val colIndex = header1.split(",").indexOf(fk)
          //creating the edges
          val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)}
          jGrah = Graph(jGrah.vertices.union(vertex1).filter((v)=>v._2!=null)
            , jGrah.edges.union(edges))
          jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

          seenDF.add((op1,jVal))
        } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
          pendingJoins.enqueue((op1, (op2, jVal)))
        }
      }
    }

    while (pendingJoins.nonEmpty) {
      val dfs_only = seenDF.map(_._1)

      val e = pendingJoins.head

      val op1 = e._1
      val op2 = e._2._1
      val jVal = e._2._2

      val vertex1 = star_df(op1).vertices
      val vertex2 = star_df(op2).vertices

      val njVal = get_NS_predicate(jVal)
      val ns = prefixes(njVal._1)

      var id1 : String = ""
      var id2 : String = ""
      var header1 : String = ""
      var header2 : String = ""

      //getting the added number to the edges ids
      if (edgeIdMap.keySet.contains(omitQuestionMark(op2)) ){
        id2 = edgeIdMap(omitQuestionMark(op2))(0)
        header2 = edgeIdMap(omitQuestionMark(op2))(1)
      }

      if (edgeIdMap.keySet.contains(omitQuestionMark(op1)) ){
        id1 = edgeIdMap(omitQuestionMark(op1))(1)
        header1 = edgeIdMap(omitQuestionMark(op1))(1)
      }

      //getting the added number to the edges ids
      if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //getting the fk column
        val colIndex = header1.split(",").indexOf(fk)
        //creating the edges
        val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk) }
        //creating the graph
        jGrah = Graph(jGrah.vertices.union(vertex2).filter((v)=>v._2!=null)
          , jGrah.edges.union(edges))
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

      } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //getting the fk column
        val colIndex = header1.split(",").indexOf(fk)
        //creating the edges
        val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)}
        jGrah = Graph(jGrah.vertices.union(vertex1).filter((v)=>v._2!=null)
          , jGrah.edges.union(edges))
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

      } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {

        pendingJoins.enqueue((op1, (op2, jVal)))
      }
      pendingJoins = pendingJoins.tail
    }


    stopwatch stop()
    val timeTaken = stopwatch.getTime
    println(s"++++++ joining time : $timeTaken")

    jGrah
    jGrah
  }

  def project(jDF: Any,
              columnNames: Seq[String],
              edgeIdMap: Map[String,Array[String]],
              distinct: Boolean): Graph[Array[String], String] = {

    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    var jGP = jDF.asInstanceOf[Graph[Array[String],String]]

    var columnIndexList: Map[String,String]= Map.empty
    var vertex: RDD[(VertexId, Array[String])] = null

    //getting columns index
    edgeIdMap.values.foreach{
      case headers =>
        columnNames.foreach{
          column=>
            if(headers(1).split(",").contains(column) && !columnIndexList.contains(headers(0) + "," + headers.indexOf(column))){
              val num : Long = headers(1).split(",").indexOf(column)
              if(columnIndexList.keySet.contains(headers(0))){
                val x = columnIndexList(headers(0))
                columnIndexList += (headers(0) ->( x + "," + num))
              }else{
                columnIndexList += (headers(0) -> num.toString)
              }

            }
        }
    }

    columnIndexList.foreach {
      index =>
        if(vertex==null){
          vertex = jGP.vertices.filter( v=>
            ((index._1+"00").equals(v._1.toString.take(3)) && index._1.toInt > -1))
            .map(v=>(v._1,
              index._2.split(",").map({i=>v._2(i.toInt)})
            ))
        }else{
          vertex = vertex.union(jGP.vertices.filter( v=>
            ((index._1+"00").equals(v._1.toString.take(3)) && index._1.toInt > -1))
            .map(v=>(v._1,
              index._2.split(",").map({i=>v._2(i.toInt)})
            )))
        }
    }

    var att = ""
    jGP.edges.collect.foreach(e => att = e.attr)

    if(vertex!=null && att!=""){
      jGP = Graph(vertex,jGP.edges)
      jGP = jGP.subgraph(vpred = (vid,vd)=>vd!=null)
      if(jGP.edges.count()==0){
        //creating temp edges
        val edge: RDD[Edge[String]] = vertex.map{(v) => Edge(v._1,v._1, "id")}
        //creating graphs
        jGP = Graph(vertex,edge)
      }
    }

    stopwatch stop()
    val timeTaken = stopwatch.getTime
    println(s"++++++ projection time : $timeTaken")

    jGP

  }

  def orderBy(jDF: Any, direction: String, variable: String, sc: Any):
  Graph[Array[String], String] = {
    jDF.asInstanceOf[Graph[Array[String], String]]
  }

  def groupBy(joinPS: Any, groupBys: (ListBuffer[String], mutable.Set[(String, String)])): Graph[Array[String],String]= {
    joinPS.asInstanceOf[Graph[Array[String],String]]
  }

  def limit(joinPS: Any, limitValue: Int): Graph[Array[String],String] = {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    joinPS.asInstanceOf[Graph[Array[String],String]]
  }

  def show(PS: Any): Unit = {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    var graph = PS.asInstanceOf[Graph[Array[String],String]]
    //graph = Graph(graph.vertices.filter((v)=>v._2!=null),graph.edges)
    // graph = graph.subgraph(vpred = (vid,vd)=>vd!=null)
      graph.triplets.
        map(triplet =>"["+ triplet.srcAttr.mkString(",") + "]")
        .collect().distinct
        .foreach(println(_))
    stopwatch stop()
    val timeTaken = stopwatch.getTime
    println(s"++++++ show time : $timeTaken")

    println(s"Number of edges: ${graph.asInstanceOf[Graph[Array[String],String]].edges.count()}")
  }

  def run(jDF: Any): Unit = {
    this.show(jDF)
  }


  def isAllDigits(x: String) = x forall Character.isDigit

  def count(joinPS: Graph[Array[String], String]): VertexId = {
    joinPS.asInstanceOf[Graph[Array[String], String]].edges.count()
  }

}