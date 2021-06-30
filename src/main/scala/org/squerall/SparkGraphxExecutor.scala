package org.squerall

import java.util
import com.google.common.collect.ArrayListMultimap
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

    var header = ""
    var FinalColumns = ""

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

      println("those are columns " + columns)

      sourceType match {
        case "csv" =>
          val data = sc.textFile(sourcePath)
          header = data.first()
          vertex =  data.filter(line => line != header)
            .map(line =>  line.split(","))
            .map( parts => ((edgeId+"00"+parts.head).toLong, parts.tail))
          val mycolumns = columns.split(",")
          val toRemove = "`".toSet
          var myheader = header.split(",")

          mycolumns.foreach(col =>
            if(myheader.contains(col.split("AS")(0).filterNot(toRemove).trim)) {
              var  i = myheader.indexOf(col.split("AS")(0).filterNot(toRemove).trim)
              myheader(i) = col.split("AS")(1).filterNot(toRemove).trim
            }
          )
          edgeIdMap = Map(str -> Array(edgeId.toString,myheader.mkString(",")))
        case _ =>
      }
      if(finalVer == null) {
        finalVer = vertex
      } else{
        finalVer = finalVer.union(vertex)
      }

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
          println("whereString : " + whereString)
          val colIndex = header.split(",").indexOf(column)-1

          if (operand_value._1 != "regex") {
            if(isAllDigits(operand_value._2)  && operand_value._1.equals("=")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toLong==operand_value._2.toLong)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toLong<operand_value._2.toLong)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toLong>operand_value._2.toLong)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">=")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toLong>=operand_value._2.toLong)
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<=")){
              finalVer = finalVer.filter {
                case (id, prop) => (prop(colIndex).toLong<=operand_value._2.toLong)
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

    val edge: RDD[Edge[String]]  = sc.parallelize( Array.empty[Edge[String]])
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
        id1 = edgeIdMap(omitQuestionMark(op1))(1)
        header1 = edgeIdMap(omitQuestionMark(op1))(1)
      }

      if (firstTime) {
        firstTime = false
        seenDF.add((op1, jVal))
        seenDF.add((op2, "ID"))
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //getting the fk column
        val colIndex = header1.split(",").indexOf(fk)-1
        //creating the edges
        val edges: RDD[Edge[String]] =vertex1
          .map{ (v) =>
            Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)
          }
        //creating the graph
        jGrah = Graph(vertex1.union(vertex2).filter((v)=>v._2!=null), edges)
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

        println("first joining edges ")
        jGrah.edges.collect().foreach(println(_))

      } else {
        val dfs_only = seenDF.map(_._1)

        if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //getting the fk column
          val colIndex = header1.split(",").indexOf(fk)-1
          //creating the edges
          val edges: RDD[Edge[String]] =vertex1
            .map{ (v) =>
              Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)
            } //creating the graph

          var mylist : mutable.MutableList[Long]=  mutable.MutableList()
          jGrah.edges.collect().foreach{
            case e =>
              if(!mylist.contains(e.dstId)){
                mylist += e.dstId
              }
              if(!mylist.contains(e.srcId)){
                mylist += e.srcId
              }
          }
          var finalED: RDD[Edge[String]] = edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId))

          println("result final ed 1")
          finalED.collect().foreach(println(_))
          println("result array 1" + mylist.mkString(", "))

          var mylist2 : mutable.MutableList[Long] =  mutable.MutableList()
          finalED.collect().foreach{
            case e =>
              if(!mylist2.contains(e.dstId)){
                mylist2 += e.dstId
              }
              if(!mylist2.contains(e.srcId)){
                mylist2 += e.srcId
              }
          }

          finalED = finalED.union(jGrah.edges.filter(e => mylist2.contains(e.dstId) || mylist2.contains(e.srcId)))

          println("result final ed 2")
          finalED.collect().foreach(println(_))
          println("result array 2" + mylist.mkString(", "))

          jGrah = Graph(jGrah.vertices.union(vertex2), finalED)
          jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

          println("2 joining edges ")
          jGrah.edges.collect().foreach(println(_))

        } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //getting the fk column
          val colIndex = header1.split(",").indexOf(fk)-1
          //creating the edges
          val edges: RDD[Edge[String]] =vertex1
            .map{ (v) =>
              Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)
            }

          var mylist : mutable.MutableList[Long]=  mutable.MutableList()
          jGrah.edges.collect().foreach{
            case e =>
              if(!mylist.contains(e.dstId)){
                mylist += e.dstId
              }
              if(!mylist.contains(e.srcId)){
                mylist += e.srcId
              }
          }

          var finalED: RDD[Edge[String]] = edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId))

          println("result final ed 1")
          finalED.collect().foreach(println(_))
          println("result array 1" + mylist.mkString(", "))

          var mylist2 : mutable.MutableList[Long] =  mutable.MutableList()

          finalED.collect().foreach{
            case e =>
              if(!mylist2.contains(e.dstId)){
                mylist2 += e.dstId
              }
              if(!mylist2.contains(e.srcId)){
                mylist2 += e.srcId
              }
          }

          finalED = finalED.union(jGrah.edges.filter(e => mylist2.contains(e.dstId) || mylist2.contains(e.srcId)))

          println("result final ed 2")
          finalED.collect().foreach(println(_))
          println("result array 2" + mylist.mkString(", "))

          jGrah = Graph(jGrah.vertices.union(vertex1).filter((v)=>v._2!=null), finalED)
          jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

          println("3 joining edges ")
          jGrah.edges.collect().foreach(println(_))

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
        val colIndex = header1.split(",").indexOf(fk)-1
        //creating the edges
        val edges: RDD[Edge[String]] =vertex1
          .map{ (v) =>
            Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)
          }

        var mylist : mutable.MutableList[Long]=  mutable.MutableList()
        jGrah.edges.collect().foreach{
          case e =>
            mylist += e.dstId
            mylist += e.srcId
        }
        var finalED: RDD[Edge[String]] = edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId))

        println("result final ed 1")
        finalED.collect().foreach(println(_))
        println("result array 1" + mylist.mkString(", "))

        mylist =  mutable.MutableList()
        finalED.collect().foreach{
          case e =>
            mylist += e.dstId
            mylist += e.srcId
        }

        finalED = finalED.union(jGrah.edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId)))

        println("result final ed 2")
        finalED.collect().foreach(println(_))
        println("result array 2" + mylist.mkString(", "))

        //creating the graph
        jGrah = Graph(jGrah.vertices.union(vertex2), finalED)
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
      } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //getting the fk column
        val colIndex = header1.split(",").indexOf(fk)-1
        //creating the edges

        val edges: RDD[Edge[String]] =vertex1
          .map{ (v) =>
            Edge(v._1,(id2+"00"+v._2(colIndex)).toLong, fk)
          }

        var mylist : mutable.MutableList[Long]=  mutable.MutableList()
        jGrah.edges.collect().foreach{
          case e =>
            mylist += e.dstId
            mylist += e.srcId
        }
        var finalED: RDD[Edge[String]] = edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId))

        println("result final ed 1")
        finalED.collect().foreach(println(_))
        println("result array 1" + mylist.mkString(", "))

        mylist =  mutable.MutableList()
        finalED.collect().foreach{
          case e =>
            mylist += e.dstId
            mylist += e.srcId
        }

        finalED = finalED.union(jGrah.edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId)))

        println("result final ed 2")
        finalED.collect().foreach(println(_))
        println("result array 2" + mylist.mkString(", "))

        //creating the graph
        jGrah = Graph(jGrah.vertices.union(vertex1), finalED)
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
      } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
        pendingJoins.enqueue((op1, (op2, jVal)))
      }

      pendingJoins = pendingJoins.tail
    }
    jGrah
  }

  def project(jDF: Any, columnNames: Seq[String], distinct: Boolean): Graph[Array[String], String] = {
    var jGP = jDF.asInstanceOf[Graph[Array[String],String]]
    //here code
    jGP
  }

  def orderBy(joinPS: Any, direction: String, variable: String, sc: Any):
  Graph[Array[String], String] = {
    var graph: Graph[Array[String],String]= null
    graph
  }

  def groupBy(joinPS: Any, groupBys: (ListBuffer[String], mutable.Set[(String, String)])): Graph[Array[String],String]= {
    joinPS.asInstanceOf[Graph[Array[String],String]]
  }

  def limit(joinPS: Any, limitValue: Int): Graph[Array[String],String] = {
    joinPS.asInstanceOf[Graph[Array[String],String]]
  }

  def show(PS: Any): Unit = {
    val graph = PS.asInstanceOf[Graph[Array[String],String]]

    println("those are the edges ")
    graph.edges.collect().foreach(println(_))
    println("those are the vertices ")
    graph.vertices.collect().foreach(println(_
    ))

    val facts2: RDD[String] = graph.triplets.map(triplet =>
      triplet.srcAttr(0)
        + " is the " + triplet.attr
        + " of " + triplet.dstAttr(0)
    )
    facts2.collect.foreach(println(_))
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