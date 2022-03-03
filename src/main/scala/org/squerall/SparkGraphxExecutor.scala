package org.squerall

import java.util
import com.google.common.collect.ArrayListMultimap
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.squerall.Helpers._
import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SparkGraphxExecutor (sparkURI: String, mappingsFile: String) extends QueryExecutorGraph[Graph[Array[String],String]] {

  def extractValues(r: ResultSet) = {  (r.getInt(1), r.getString(2))}

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
  (Graph[Array[String],String],Integer,String,Map[String,Array[String]],SparkContext)  = {
    var spark = SparkSession.builder.master(sparkURI).appName("Squerall").getOrCreate
    var sc = spark.sparkContext
    var dataSource_count = 0
    var parSetId = ""
    var finalVer: RDD[(VertexId, Array[String])] = null

    var finalHeaderIndex : Array[Int] = Array.empty
    var selectedHeader: Array[String]=Array.empty
    var finalHeader: Array[String] = Array.empty
    var finalHeader2: Array[String] = Array.empty
    var edgeIdMap : Map[String, Array[String]] = Map.empty

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

      //getting columns names
      val toRemove = "`".toSet
      columns.split(",").foreach (col => finalHeader = finalHeader :+ col.split("AS")(1).filterNot(toRemove).trim)
      columns.split(",").foreach {
        col =>
          if(col.contains("_")){
            finalHeader2 = finalHeader2 :+ col.split("AS")(1).split("_")(1).filterNot(toRemove).trim
          }else{
            finalHeader2 = finalHeader2 :+ col.split("AS")(1).filterNot(toRemove).trim
          }
      }
      columns.split(",").foreach (col => selectedHeader = selectedHeader :+ col.split("AS")(0).filterNot(toRemove).trim.toLowerCase)//toLowerCase for mongodb since it is case sensitive
      //vertex
      var vertex: RDD[(VertexId, Array[String])] = null
      var header = ""

      sourceType match {
        case "csv" =>
          val data = sc.textFile(sourcePath)
          //getting the header
          header = data.first()
          finalHeaderIndex = getColumnsIndex(header,selectedHeader,toRemove)
          //getting data
          val all_col_vertex =  data .filter(line => line != header).map(line =>  line.split(",")).map( parts => ((edgeId+"00"+parts.head).toLong, parts))
          vertex = all_col_vertex.map(v=>(v._1,finalHeaderIndex.map({i=>v._2(i)})))
        case "cassandra" =>
          import com.datastax.spark.connector._
          //start new spark contet
          val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
          sc.stop()
          spark = SparkSession.builder.master(sparkURI).appName("Squerall").config(conf).getOrCreate
          sc = spark.sparkContext
          val values = options.values.toList
          val rdd = sc.cassandraTable(keyspace = values(0), table = values(1))
          vertex = rdd.map(r=>((edgeId+"00"+r.getLong("id").toString).toLong,selectedHeader.map({i=>r.getString(i)})))
        case "mongodb" =>
          val values = options.values.toList
          val mongoConf = if (values.length == 4) makeMongoURI(values(0), values(1), values(2), values(3))
          else makeMongoURI(values(0), values(1), values(2), null)
          val mongoOptions: ReadConfig = ReadConfig(Map("uri" -> mongoConf, "partitioner" -> "MongoPaginateBySizePartitioner"))
          val rdd = MongoSpark.load(sc,mongoOptions)
          vertex =  rdd.filter(line => line != header).map(d=>((edgeId+"00"+d.get("id").toString).toLong,selectedHeader.map({i=>d.get(i).toString})))
        case "jdbc" =>
          val jdbcRDD = LoadSimpleJdbc.getResults(sc,selectedHeader,options)
          vertex = jdbcRDD.map(j=>((edgeId+"00"+j._1.toString).toLong,j._2))
        case "neo4j" =>
          import org.neo4j.spark._
          //starting new spark context
          sc.stop()
          val config = new SparkConf()
          config.set(Neo4jConfig.prefix + "url", "bolt://localhost")
          config.set(Neo4jConfig.prefix + "user", "neo4j")
          config.set(Neo4jConfig.prefix + "password", "test")
          spark = SparkSession.builder.master(sparkURI).appName("Squerall").config(config).getOrCreate
          sc = spark.sparkContext
          var selected_columns = ""
          selectedHeader.foreach(col =>
            selected_columns = selected_columns  + " var."+ selectedHeader + ","
          )
          if(!selected_columns.contains("nr")){
            selected_columns = " var.nr, " + selected_columns
          }
          selected_columns = selected_columns.dropRight(1)
          val values = options.values.toList
          val table_name = values(1)
          val neo = Neo4j(sc)
          val rdd = neo.cypher("MATCH (var:"+table_name+") RETURN " + selected_columns).loadRowRdd
          vertex = rdd.map(r=>((edgeId+"00"+r.getLong(0).toString).toLong,finalHeaderIndex.map({i=>r.get(i).toString})))
      }
      if(finalVer == null) {
        finalVer = vertex
      } else{
        finalVer = finalVer.union(vertex)
      }
      edgeIdMap = Map(str -> Array(edgeId.toString,finalHeader.mkString(","),finalHeader2.mkString(",")))
    }

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
          val colIndex = finalHeader.indexOf(column)

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
           edgeIdMap: Map[String,Array[String]])
  :Graph[Array[String],String] = {
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
          //creating the edgesmap
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
    jGrah
  }

  def project(jDF: Any, columnNames: Seq[String], edgeIdMap: Map[String,Array[String]], orderby: Boolean):
  (Graph[Array[String], String], Map[String,Array[String]])= {

    var jGP = jDF.asInstanceOf[Graph[Array[String],String]]

    var columnIndexList: Map[String,Array[String]]= Map.empty
    var vertex: RDD[(VertexId, Array[String])] = null

    var edgeIdMap2: Map[String,Array[String]] = Map.empty

    println("column names " + columnNames.mkString(","))
    println("edgeIdMap " + edgeIdMap.keySet.mkString(","))
    edgeIdMap.values.foreach(v => println(v.mkString(",")))
    //getting columns index
    edgeIdMap.values.foreach{
      case headers =>
        columnNames.foreach{
          case column=>
            if(headers(1).split(",").contains(column) && !columnIndexList.contains(headers(0) + "," + headers(2).split(",").indexOf(column))){
              val num : Long = headers(1).split(",").indexOf(column)
              if(columnIndexList.keySet.contains(headers(0))){
                val x = columnIndexList(headers(0))
                columnIndexList += (headers(0) ->Array( x(0) + "," + num,x(1)+","+ column))
              }else{
                columnIndexList += (headers(0) -> Array(num.toString,column))
              }
            }
        }
    }
    println("keys " + columnIndexList.keySet.mkString(","))
    columnIndexList.values.foreach(v=>println(v.mkString(",")))
    var i : Int = 0
    columnIndexList.foreach {
      index =>
        if(vertex==null){
          vertex = jGP.vertices.filter( v=>
            ((index._1+"00").equals(v._1.toString.take(3)) && index._1.toInt > -1))
            .map(v=>(v._1, index._2(0).split(",").map({i=>v._2((i.toInt))})))
        }else{
          vertex = vertex.union(jGP.vertices.filter( v=>
            ((index._1+"00").equals(v._1.toString.take(3)) && index._1.toInt > -1))
            .map(v=>(v._1, index._2(0).split(",").map({i=>v._2(i.toInt)})
            )))
        }
        //getting indexes of columns after projection
        index._2(1).split(",").foreach{
          x =>
            edgeIdMap2 += (i.toString -> Array(i.toString,x))
            i = i + 1
        }
    }

    // var att = ""
    //jGP.edges.collect.foreach(e => att = e.attr)

    if(vertex!=null/* && att!=""*/){
      val edges = jGP.edges
      jGP = Graph(vertex,edges)
      //jGP = jGP.subgraph(vpred = (vid,vd)=>vd!=null)
      if(jGP.edges.count()==0){
        //creating temp edges
        val edge: RDD[Edge[String]] = vertex.map{(v) => Edge(v._1,v._1, "id")}
        //creating graphs
        jGP = Graph(vertex,edge)
      }
    }

    (jGP,edgeIdMap2)
  }

  def orderBy(jDF: Any, direction: String, variable: String):
  Graph[Array[String], String] = {
    val graph = jDF.asInstanceOf[Graph[Array[String], String]]
    graph
  }

  def groupBy(joinPS: Any, groupBys: (ListBuffer[String], mutable.Set[(String, String)])): Graph[Array[String],String]= {
    joinPS.asInstanceOf[Graph[Array[String],String]]
  }

  def limit(joinPS: Any, limitValue: Int): Graph[Array[String],String] = {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    joinPS.asInstanceOf[Graph[Array[String],String]]
  }

  def show(PS: Any, variable: String, edgeIdMap: Map[String,Array[String]], limit: Int, orderby: Boolean, distinct: Boolean): Double = {
    val graph = PS.asInstanceOf[Graph[Array[String],String]]
    var triples = graph.triplets
    if(orderby){
      val dex= getSingleColumnIndex(edgeIdMap,variable)
      triples =  triples.sortBy(_.srcAttr(dex))
    }
    var results = triples.map(triplet =>"["+ triplet.srcAttr.mkString(",") + "]").collect()
    if(distinct) {
      results = results.distinct
    }
    if(limit>0) {
      results = results.take(limit)
    }
    results.take(10).foreach(println(_))
    //  println(s"Number of edges: ${graph.asInstanceOf[Graph[Array[String],String]].edges.count()}")
    (results.length)
  }


  def run(jDF: Any, variable: String, edgeIdMap: Map[String,Array[String]], limit: Int, orderby: Boolean, distinct : Boolean): Double = {
    this.show(jDF,  variable, edgeIdMap, limit, orderby, distinct)
  }


  def isAllDigits(x: String) = x forall Character.isDigit

  def count(joinPS: Graph[Array[String], String]): VertexId = {
    joinPS.asInstanceOf[Graph[Array[String], String]].edges.count()
  }

  def getColumnsIndex(header: String, columns: Array[String], toRemove: Set[Char]) : Array[Int] = {
    var finalHeaderIndex:Array[Int]= Array.empty
    val headerArray = header.toLowerCase().split(",")
    columns.foreach { col =>
      if (headerArray.contains(col)) {
        val i = headerArray.indexOf(col)
        finalHeaderIndex = finalHeaderIndex :+ i
      }
    }
    finalHeaderIndex
  }
  //        edgeIdMap2 += (index._1 -> Array(i,index._2(1)))
  def getSingleColumnIndex(edgeIdMap : Map[String, Array[String]] , variable : String) : Int =
  {
    import scala.util.control.Breaks._
    //var columnIndexList: Map[String,String]= Map.empty
    var num: Int = 0
    edgeIdMap.values.foreach{
      headers =>
        if(headers(1).contains(variable)){
          num = headers(0).toInt
        }
    }
    num
  }
}