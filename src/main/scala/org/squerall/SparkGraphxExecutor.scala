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
    import org.neo4j.spark._
    //starting new spark context
    val config = new SparkConf()
    config.set(Neo4jConfig.prefix + "url", "bolt://localhost:11007")
    config.set(Neo4jConfig.prefix + "user", "neo4j")
    config.set(Neo4jConfig.prefix + "password", "test")
    config.set("spark.cassandra.connection.host", "127.0.0.1")

    val spark = SparkSession.builder.master(sparkURI).appName("Squerall").config(config).getOrCreate

    var sc = spark.sparkContext
    var dataSource_count = 0
    var parSetId = ""
    var finalVer: RDD[(VertexId, Array[String])] = null

    var finalHeaderIndex : Array[Int] = Array.empty
    var selectedHeader: Array[String]=Array.empty
    var selectedHeader2: Array[String]=Array.empty
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
      columns.split(",").foreach (col => selectedHeader2 = selectedHeader2 :+ col.split("AS")(0).filterNot(toRemove).trim)//toLowerCase for mongodb since it is case sensitive

      //vertex
      var vertex: RDD[(VertexId, Array[String])] = null
      var header = ""

      sourceType match {
        case "csv" =>
          val data = sc.textFile(sourcePath)
          //getting the header
          header = data.first()
          finalHeaderIndex = getColumnsIndex(header,selectedHeader,toRemove)
     //     println("this is finalHeaderIndex " + finalHeaderIndex.mkString(","))
          //getting data
          val all_col_vertex =  data .filter(line => line != header).map(line =>  line.split(",")).map( parts => ((edgeId+"00"+parts.head).toLong, parts))
          vertex = all_col_vertex.map(v=>(v._1,finalHeaderIndex.map({i=>v._2(i)})))
        case "cassandra" =>
          import com.datastax.spark.connector._
          //start new spark contet
          val values = options.values.toList
          val rdd = sc.cassandraTable(keyspace = values(0), table = values(1))
          vertex = rdd.map(r=>((edgeId+"00"+r.getLong("id").toString).toLong,selectedHeader.map({i=>r.getString(i)})))
        case "mongodb" =>
          val values = options.values.toList
          val mongoConf = if (values.length == 4) makeMongoURI(values(0), values(1), values(2), values(3))
          else makeMongoURI(values(0), values(1), values(2), null)
          val mongoOptions: ReadConfig = ReadConfig(Map("uri" -> mongoConf, "partitioner" -> "MongoPaginateBySizePartitioner"))
          val rdd = MongoSpark.load(sc,mongoOptions)
          vertex =  rdd.map(d=>((edgeId+"00"+d.get("id").toString).toLong,selectedHeader2.map({i=>d.get(i).toString})))
        case "jdbc" =>
          val jdbcRDD = LoadSimpleJdbc.getResults(sc,selectedHeader,options)
          vertex = jdbcRDD.map(j=>((edgeId+"00"+j._1.toString).toLong,j._2))
        case "neo4j" =>
          var selected_columns = ""
          selectedHeader.foreach(col =>
            selected_columns = selected_columns  + " var."+ col + ","
          )
          if(!selected_columns.contains("nr")){
            selected_columns = " var.nr, " + selected_columns
          }
          selected_columns = selected_columns.dropRight(1)
          val values = options.values.toList
          val table_name = values(1)
          val neo = Neo4j(sc)
          val rdd = neo.cypher("MATCH (var:"+table_name+") RETURN " + selected_columns).loadRowRdd
          println("selected_columns " + selected_columns)
          val id_index = selected_columns.trim.split(",").indexOf(" var.nr")
          rdd.take(1).foreach(v=>println("** " + v.get(id_index).toString))
          vertex = rdd.map(r=>((edgeId+"00"+r.get(id_index).toString).toLong,r.mkString(",").split(",")))
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
           edgeIdMap: Map[String,Array[String]],
           columnNames: Seq[String])

  :(Graph[Array[String],String],String) = {
    import scala.collection.JavaConversions._
    import scala.collection.mutable.ListBuffer

    var pendingJoins = mutable.Queue[(String, (String, String))]()
    val seenDF : ListBuffer[(String,String)] = ListBuffer()
    var firstTime = true
    var jGrah :Graph[Array[String],String] = null
    var finalHeader : String = ""

    val it = joins.entries.iterator
    while ({it.hasNext}) {
      val entry = it.next

      val op1 = entry.getKey
      val op2 = entry.getValue._1
      val jVal = entry.getValue._2

      var vertex1 = star_df(op1).vertices
      val vertex2 = star_df(op2).vertices

      val njVal = get_NS_predicate(jVal)
      val ns = prefixes(njVal._1)

      var id1 : String = ""
      var id2 : String = ""
      var header1 : String = ""
      var header2 : String = ""
      var project1 : Array[Int] = Array.empty
      var project2 : Array[Int] = Array.empty
      it.remove()

    //  println("this is columns " + columnNames)
      //getting the added number to the edges ids
      if (edgeIdMap.keySet.contains(omitQuestionMark(op1)) ){
        id1 = edgeIdMap(omitQuestionMark(op1))(0)
        header1 = edgeIdMap(omitQuestionMark(op1))(1)
   //     println("this is header1 " + header1)
        project1 = getColumnsIndex(header1,columnNames.toArray,null)
 //       println("this is projet1 " + project1.mkString(","))
      }

      if (edgeIdMap.keySet.contains(omitQuestionMark(op2)) ){
        id2 = edgeIdMap(omitQuestionMark(op2))(0)
        header2 = edgeIdMap(omitQuestionMark(op2))(1)
    //    println("this is header2 " + header2)
        project2 = getColumnsIndex(header2,columnNames.toArray,null)
   //     println("this is projet2 " + project2.mkString(","))
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
        //projection
        val vertex1_1 = vertex1.map(v=>(v._1, project1.map({i=>v._2(i)})))
     //   vertex1_1.foreach(v=>println(v._1 + " , " + v._2.mkString(",")))
        val vertex2_2 = vertex2.map(v=>(v._1, project2.map({i=>v._2(i)})))
     //   vertex2_2.foreach(v=>println(v._1 +" , " + v._2.mkString(",")))
    //    edges.foreach(e=>println(e.srcId + "," + e.dstId))
        //creating the graph
        jGrah = Graph(vertex1_1.union(vertex2_2).filter((v)=>v._2!=null)
          , edges)
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
        //getting triplet
      //  println("this is first time")
      //  jGrah.triplets.foreach(t=>println(t.srcAttr.mkString(",")))
        val newVertex = jGrah.triplets.map(t=>(t.srcId,(t.srcAttr.mkString(",") + "," +t.dstAttr.mkString(",")).split(",")))
        val newEdge: RDD[Edge[String]] = newVertex.map{(v) => Edge(v._1,v._1, "id")}
        jGrah = Graph(newVertex,newEdge)
     //   println("this is first time")
     //   jGrah.triplets.foreach(t=>println(t.srcAttr.mkString(",")))
        finalHeader = getVertexAttributs(header1,columnNames.toArray)+ ","   + getVertexAttributs(header2,columnNames.toArray)
    //    println("finalHeader finalHeader finalHeader " + finalHeader)
      } else {
        val dfs_only = seenDF.map(_._1)

        if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //getting the fk column
          val colIndex = header1.split(",").indexOf(fk)
          //creating the edgesmap
          val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk) }
         // val vertex1_1 = vertex1.map(v=>(v._1, project1.map({i=>v._2(i)})))
          val vertex2_2 = vertex2.map(v=>(v._1, project2.map({i=>v._2(i)})))
          //creating the graph
          //jGrah = Graph(jGrah.vertices.union(vertex2).filter((v)=>v._2!=null)
         //   , jGrah.edges.union(edges))
          jGrah = Graph(jGrah.vertices.union(vertex2_2).filter((v)=>v._2!=null)
            , edges)
          jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
        //getting triplet
          val newVertex = jGrah.triplets.map(t=>(t.srcId,(t.srcAttr.mkString(",") + "," +t.dstAttr.mkString(",")).split(",")))
          val newEdge: RDD[Edge[String]] = newVertex.map{(v) => Edge(v._1,v._1, "id")}
          jGrah = Graph(newVertex,newEdge)
      //    println("this is second time")
      //    jGrah.triplets.foreach(t=>println(t.srcAttr.mkString(",")))
          seenDF.add((op2,"ID"))
          finalHeader =  finalHeader + "," +  getVertexAttributs(header2,columnNames.toArray)
      //    println("finalHeader finalHeader finalHeader " + finalHeader)
        } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //getting the fk column
          val colIndex = header1.split(",").indexOf(fk)
          //creating the edges
          val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)}
          val vertex1_1 = vertex1.map(v=>(v._1, project1.map({i=>v._2(i)})))
          //val vertex2_2 = vertex2.map(v=>(v._1, project2.map({i=>v._2(i)})))
          //getting temp graph
          jGrah = Graph(jGrah.vertices.union(vertex1_1).filter((v)=>v._2!=null)
            , edges)
          jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
         //getting triplet
         val newVertex = jGrah.triplets.map(t=>(t.srcId,(t.srcAttr.mkString(",") + "," +t.dstAttr.mkString(",")).split(",")))
          val newEdge: RDD[Edge[String]] = newVertex.map{(v) => Edge(v._1,v._1, "id")}
          jGrah = Graph(newVertex,newEdge)
        //  println("this is third time")
          jGrah.triplets.foreach(t=>print(t.srcAttr.mkString(",")))
          seenDF.add((op1,jVal))
          finalHeader =  finalHeader + "," +  getVertexAttributs(header1,columnNames.toArray)
      //    println("finalHeader finalHeader finalHeader " + finalHeader)
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
      //getting triplet
      val newVertex = jGrah.triplets.map(t=>(t.srcId,(t.srcAttr.mkString(",") + "," +t.dstAttr.mkString(",")).split(",")))
        val newEdge: RDD[Edge[String]] = newVertex.map{(v) => Edge(v._1,v._1, "id")}
        jGrah = Graph(newVertex,newEdge)
      } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //getting the fk column
        val colIndex = header1.split(",").indexOf(fk)
        //creating the edges
        val edges: RDD[Edge[String]] =vertex1.map{ (v) => Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)}
        jGrah = Graph(jGrah.vertices.union(vertex1).filter((v)=>v._2!=null)
          , jGrah.edges.union(edges))
        //jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
        //getting triplet
        val newVertex = jGrah.triplets.map(t=>(t.srcId,(t.srcAttr.mkString(",") + "," +t.dstAttr.mkString(",")).split(",")))
        val newEdge: RDD[Edge[String]] = newVertex.map{(v) => Edge(v._1,v._1, "id")}
        jGrah = Graph(newVertex,newEdge)
      } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {

        pendingJoins.enqueue((op1, (op2, jVal)))
      }
      pendingJoins = pendingJoins.tail
    }
    (jGrah,finalHeader)
  }

def project(jDF: Any, columnNames: Seq[String],  edgeIdMap: Map[String,Array[String]]):
(Graph[Array[String], String],String)= {
  var jGP = jDF.asInstanceOf[Graph[Array[String], String]]
  var finalHeader: String = ""
  var finalHeaderIndex: Array[Int] = Array.empty
  var vertex : RDD[(VertexId,Array[String])] = null
  val edges = jGP.edges
  edgeIdMap.foreach{
    key =>
    //  println("this is key " + key._1 + ", " + key._2.mkString )
      val header = key._2(1)
      finalHeader = getVertexAttributs(header,columnNames.toArray)
   //   println("final header " + finalHeader)
      finalHeaderIndex = getColumnsIndex(header,columnNames.toArray,null)
   //   println("final header inde" + finalHeaderIndex.mkString(","))
  }
  vertex = jGP.vertices.map(v=>(v._1, finalHeaderIndex.map({i=>v._2(i)})))
  jGP = Graph(vertex, edges)
  (jGP,finalHeader)
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
    joinPS.asInstanceOf[Graph[Array[String],String]]
  }

  def show(PS: Any, variable: String, limit: Int, orderby: Boolean, distinct: Boolean, finalHeader : String): Double = {
    val graph = PS.asInstanceOf[Graph[Array[String], String]]

    var triples = graph.triplets
    //println("this is triplet")
  //  triples.foreach(t=>println(t.srcAttr.mkString(",") + " ,, "+t.dstAttr.mkString(",")))
      if (orderby) {
        val dex = getSingleColumnIndex(finalHeader, variable)
       // println("this is finalHeader " + finalHeader)
       // println("this is dex " + dex)
        triples = triples.sortBy(_.srcAttr(dex))
      }
      var results = triples.map(triplet => "[" + triplet.srcAttr.mkString(",") + "]").collect()
      if (distinct) {
        results = results.distinct
      }
      if (limit > 0) {
        results = results.take(limit)
      }
      results.foreach(println(_))
      //  println(s"Number of edges: ${graph.asInstanceOf[Graph[Array[String],String]].edges.count()}")
      (results.length)
  }

  def run(jDF: Any, variable: String, limit: Int, orderby: Boolean, distinct : Boolean, finalHeader :String): Double = {
    this.show(jDF,  variable, limit, orderby, distinct, finalHeader)
  }


  def isAllDigits(x: String) = x forall Character.isDigit

  def count(joinPS: Graph[Array[String], String]): VertexId = {
    joinPS.asInstanceOf[Graph[Array[String], String]].edges.count()
  }

  def getColumnsIndex(header: String, columns: Array[String], toRemove: Set[Char]) : Array[Int] = {
    var finalHeaderIndex:Array[Int]= Array.empty
    val headerArray = header.toLowerCase().split(",")
    columns.foreach { col =>
     // println("this is col " + col)
    //  println("this is headerArray " + headerArray.mkString(","))
    //  println((headerArray.contains(col.toLowerCase())))
      if (headerArray.contains(col.toLowerCase())) {
        val i = headerArray.indexOf(col.toLowerCase())
        finalHeaderIndex = finalHeaderIndex :+ i
      }
    }
    finalHeaderIndex
  }
  //        edgeIdMap2 += (index._1 -> Array(i,index._2(1)))
  def getSingleColumnIndex(finalHeader : String , variable : String) : Int =
  {
    import scala.util.control.Breaks._
    //var columnIndexList: Map[String,String]= Map.empty
    var fh = finalHeader.toLowerCase.split(",")
    //println("this is finalheder " + finalHeader)
    var num = -1
    fh.foreach{
      headers =>
        //println("this is headers " + headers + " this is variable " + variable)
        if(headers.equals(variable.toLowerCase)){
          //println("this is num")
          num = fh.indexOf(variable.toLowerCase)
        }
    }
    num
  }

  def getVertexAttributs(header: String, columns: Array[String]) : String = {
    var finalHeader: String= ""
    val headerArray = header.toLowerCase().split(",")
    columns.foreach { col =>
   //   println("this is col " + col)
   //   println("this is headerArray " + headerArray.mkString(","))
  //    println((headerArray.contains(col.toLowerCase())))
      if (headerArray.contains(col.toLowerCase())) {
        finalHeader = finalHeader + "," + col
      }
    }
    if(!finalHeader.equals("")){
      finalHeader =  finalHeader.substring(1)
    }

    finalHeader
  }
}