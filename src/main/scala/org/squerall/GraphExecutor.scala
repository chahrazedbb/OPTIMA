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

class GraphExecutor (sparkURI: String, mappingsFile: String) extends QueryExecutorGraph[Graph[String,String]] {

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
  (Graph[String,String],Integer,String,Map[String,Array[String]],Any)  = {

    val spark = SparkSession.builder.master(sparkURI).appName("Squerall").getOrCreate
    val sc = spark.sparkContext
    var edgeIdMap : Map[String, Array[String]] = Map.empty
    var dataSource_count = 0
    var parSetId = ""
    var vertex: RDD[(VertexId, String)] = null
    var edge: RDD[Edge[String]] = null
    var finalVer: RDD[(VertexId, String)] = null
    var header: Array[String] = null
    var FinalColumns = ""
    var finalColumns: Array[String] = null

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
          //getting the column names
          header = data.first().split(",")
          val mycolumns = columns.split(",")
          val toRemove = "`".toSet
          mycolumns.foreach(col =>
            if(header.contains(col.split("AS")(0).filterNot(toRemove).trim)) {
              var  i = header.indexOf(col.split("AS")(0).filterNot(toRemove).trim)
              header(i) = col.split("AS")(1).filterNot(toRemove).trim
            }
          )



          println("cccccccccccccccccccccccccccccc")
          header.foreach(println(_))

          //extracing data
          header.foreach{
            case (column) =>
              var index = header.indexOf(column)
              var head = data.first()
              var datagraph = data.filter(line => line != head)
                .map(line =>  line.split(","))
              if(vertex == null){
                //extracting vertices
                vertex =  datagraph.map(line=>((edgeId+"00"+line(0)).toLong,(line(index))))
                //extracting edges
                edge = datagraph.map(line => Edge((edgeId+"00"+line(0)).toLong,(edgeId+"00"+line(0)).toLong,column))
              }else{
                vertex = vertex.union(datagraph
                  .map(line=>((edgeId+"00"+index+line(0)).toLong,(line(index)))))
                //extracting edges
                edge = edge.union(datagraph
                  .map((line) => Edge((edgeId+"00"+line(0)).toLong,(edgeId+"00"+index+line(0)).toLong,column)))
              }
          }
          edgeIdMap = Map(str -> Array(edgeId.toString,header.mkString(",")))
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

          //getting the column values
          var mylist : mutable.MutableList[Long]=  mutable.MutableList()
          edge.collect().foreach{
            case e =>
              if(e.attr.equals(column)){
                mylist += e.dstId
              }
          }

          if (operand_value._1 != "regex") {
            if(isAllDigits(operand_value._2)  && operand_value._1.equals("=")){
              finalVer = finalVer.filter {
                case (id, prop) => ((mylist.contains(id) && prop.toLong==operand_value._2.toLong) || !mylist.contains(id))
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<")){
              finalVer = finalVer.filter {
                case (id, prop) => (mylist.contains(id) && prop.toLong<operand_value._2.toLong || !mylist.contains(id))
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">")){
              finalVer = finalVer.filter {
                case (id, prop) => (mylist.contains(id) && prop.toLong>operand_value._2.toLong || !mylist.contains(id))
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">=")){
              finalVer = finalVer.filter {
                case (id, prop) => (mylist.contains(id) && prop.toLong>=operand_value._2.toLong || !mylist.contains(id))
                case _ => false
              }
            }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<=")){
              finalVer = finalVer.filter {
                case (id, prop) => (mylist.contains(id) && prop.toLong<=operand_value._2.toLong || !mylist.contains(id))
                case _ => false
              }
            }else{
              finalVer = finalVer.filter {
                case (id, prop) => (mylist.contains(id) && prop.equals(operand_value._2.split("\"")(1)) || !mylist.contains(id))
                case _ => false
              }
            }
          }
          // else  finalGP = finalGP.filter(finalGP(column).like(operand_value._2.replace("\"","")))
          // regular expression with _ matching an arbitrary character and % matching an arbitrary sequence
        }
      }
    }

    /*    //creating the edges
        val edge: RDD[Edge[String]] =finalVer
          .map{ (v) =>
            Edge(v._1,v._1, "id")
          }
    */
    var graph:Graph[String,String] = Graph(finalVer,edge)

    (graph, nbrOfFiltersOfThisStar, parSetId, edgeIdMap, sc)
  }

  def transform(ps: Any, column: String, transformationsArray: Array[String]): Any = {
    ps.asInstanceOf[Graph[String,String]]
  }

  def join(joins: ArrayListMultimap[String, (String, String)],
           prefixes: Map[String, String],
           star_df: Map[String, Graph[String,String]],
           edgeIdMap: Map[String,Array[String]],
           sc: Any)
  :Graph[String,String] = {
    import scala.collection.JavaConversions._
    import scala.collection.mutable.ListBuffer

    var pendingJoins = mutable.Queue[(String, (String, String))]()
    val seenDF : ListBuffer[(String,String)] = ListBuffer()
    var firstTime = true
    var jGrah :Graph[String,String] = null

    val it = joins.entries.iterator
    while ({it.hasNext}) {
      val entry = it.next

      val op1 = entry.getKey
      val op2 = entry.getValue._1
      val jVal = entry.getValue._2

      val graph1 = star_df(op1)
      val graph2 = star_df(op2)

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
        //creating the new edges
        println("this is fk " + fk)
        val tempEdges: RDD[Edge[String]] =graph1.edges.filter(e=>e.attr.equals(fk))
        //filtering the edges
        var mytemplist : mutable.MutableList[String]=  mutable.MutableList()
        tempEdges.collect().foreach{
          case e =>
            if(!mytemplist.contains(e.dstId)){
              mytemplist += e.dstId.toString.substring(1)
            }
        }
        mytemplist.foreach(println(_))
        println("id1 " + id1  + " id2 " + id2)
        graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1))).collect().foreach(println(_))
        val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1))).map{ (v) =>
            Edge((id1+"00"+v._1.toString.substring(4)).toLong,(id2+"00"+v._2).toLong, fk)
          }
        //creating the graph
        println("******************* yep *************")
        edges.collect().foreach(println(_))
        //filtering the edges
        var mylist : mutable.MutableList[String]=  mutable.MutableList()
        println("******************* yep *************")
        edges.collect().foreach{
          case e =>
            if(!mylist.contains(e.dstId)){
              mylist += e.dstId.toString
            }
        }
        var finalED: RDD[Edge[String]] = graph2.edges.filter(e => mylist.contains(e.srcId.toString))
        println("******************* yep *************")
        finalED.collect().foreach(println(_))
        //creating the graph
        jGrah = Graph(graph1.vertices.union(graph2.vertices).filter((v)=>v._2!=null),
          finalED.union(graph1.edges))
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

        println("first joining")
        jGrah.edges.collect().foreach(println(_))

      } else {
        val dfs_only = seenDF.map(_._1)

        if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //creating the new edges
          println("this is fk " + fk)
          val tempEdges: RDD[Edge[String]] =graph1.edges.filter(e=>e.attr.equals(fk))
          //filtering the edges
          var mytemplist : mutable.MutableList[String]=  mutable.MutableList()
          tempEdges.collect().foreach{
            case e =>
              if(!mytemplist.contains(e.dstId)){
                mytemplist += e.dstId.toString.substring(1)
              }
          }
          val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1))).map{ (v) =>
            Edge((id1+"00"+v._1.toString.substring(4)).toLong,(id2+"00"+v._2).toLong, fk)
          }

          var attId = ""
          //creating the graph
          var mylist : mutable.MutableList[String]=  mutable.MutableList()
          edges.collect().foreach{
            case e =>
              if(!mylist.contains(e.dstId)){
                mylist += e.dstId.toString
              }
               attId = e.dstId.toString.split("00")(0)
          }

          var finalED: RDD[Edge[String]] = graph2.edges.filter(e =>
            (attId.equals(e.srcId.toString.split("00")(0)) &&  mylist.contains(e.srcId))
            || !attId.equals(e.srcId.toString.split("00")(0)))

         jGrah = Graph(jGrah.vertices.union(graph2.vertices).filter((v)=>v._2!=null), jGrah.edges.union(finalED))
          jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

          seenDF.add((op2,"ID"))

          println("second joining")
          jGrah.edges.collect().foreach(println(_))

        } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //creating the edges
          println("this is fk " + fk)
          val tempEdges: RDD[Edge[String]] =graph1.edges.filter(e=>e.attr.equals(fk))
          //filtering the edges
          var mytemplist : mutable.MutableList[String]=  mutable.MutableList()
          tempEdges.collect().foreach{
            case e =>
              if(!mytemplist.contains(e.dstId)){
                mytemplist += e.dstId.toString.substring(1)
              }
          }
          val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1))).map{ (v) =>
            Edge((id1+"00"+v._1.toString.substring(4)).toLong,(id2+"00"+v._2).toLong, fk)
          }
          var attId = ""
          //creating the graph
          var mylist : mutable.MutableList[String]=  mutable.MutableList()
          edges.collect().foreach{
            case e =>
              if(!mylist.contains(e.dstId)){
                mylist += e.dstId.toString
              }
              attId = e.dstId.toString.split("00")(0)
          }

          var finalED: RDD[Edge[String]] = jGrah.edges.filter(e =>
            (attId.equals(e.srcId.toString.split("00")(0)) &&  mylist.contains(e.srcId.toString))
            || !attId.equals(e.srcId.toString.split("00")(0)))

          jGrah = Graph(jGrah.vertices.union(graph1.vertices).filter((v)=>v._2!=null),
            graph1.edges.union(finalED))
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

      val graph1 = star_df(op1)
      val graph2 = star_df(op2)

      val njVal = get_NS_predicate(jVal)
      val ns = prefixes(njVal._1)

      var id1 : String = ""
      var id2 : String = ""
      var header1 : String = ""
      var header2 : String = ""


      println("hi there 00")


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
        val edges: RDD[Edge[String]] =jGrah.edges.filter(e=>e.attr.equals(fk))

        //creating the graph
        var mylist : mutable.MutableList[Long]=  mutable.MutableList()
        edges.collect().foreach{
          case e =>
            if(!mylist.contains(e.dstId)){
              mylist += e.dstId
            }
            if(!mylist.contains(e.srcId)){
              mylist += e.srcId
            }
        }
        var finalED: RDD[Edge[String]] = graph2.edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId))

        jGrah = Graph(jGrah.vertices.union(graph2.vertices),
          jGrah.edges.union(finalED))
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)
      } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //getting the fk column
        val colIndex = header1.split(",").indexOf(fk)-1
        //creating the edges
        val edges: RDD[Edge[String]] =graph1.edges.filter(e=>e.attr.equals(fk))

        var mylist : mutable.MutableList[Long]=  mutable.MutableList()
        edges.collect().foreach{
          case e =>
            if(!mylist.contains(e.dstId)){
              mylist += e.dstId
            }
            if(!mylist.contains(e.srcId)){
              mylist += e.srcId
            }
        }

        var finalED: RDD[Edge[String]] = jGrah.edges.filter(e => mylist.contains(e.dstId) || mylist.contains(e.srcId))

        jGrah = Graph(jGrah.vertices.union(graph1.vertices).filter((v)=>v._2!=null),
          graph1.edges.union(finalED))
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

      } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
        println("hi there 33")

        pendingJoins.enqueue((op1, (op2, jVal)))
      }
      pendingJoins = pendingJoins.tail
    }
    jGrah
  }

  def project(jDF: Any, columnNames: Seq[String],  edgeIdMap: Map[String,Array[String]],distinct: Boolean): Graph[String, String] = {
    var jGP = jDF.asInstanceOf[Graph[String,String]]
    var edges: RDD[Edge[String]] = null


    println("cccccccccccccccccccccccccccccc")
    columnNames.foreach(println(_))

    for(name <- columnNames){
      if(edges == null){
        edges = jGP.edges.filter {
          case Edge(_, _, label) => label.equals(name)
        }
      }else{
        edges = edges.union(jGP.edges.filter {
          case Edge(_, _, label) => label.equals(name)
        })
      }
    }

    if(distinct){
      jGP = Graph(
        jGP.vertices.distinct(),
        edges.distinct()
      )
    }else{
      jGP = Graph(
        jGP.vertices,
        edges
      )
    }

    jGP
  }

  def orderBy(joinPS: Any, direction: String, variable: String, sc: Any):
  Graph[String, String] = {
    joinPS.asInstanceOf[Graph[String,String]]
  }

  def groupBy(joinPS: Any, groupBys: (ListBuffer[String], mutable.Set[(String, String)])): Graph[String,String]= {
    joinPS.asInstanceOf[Graph[String,String]]
  }

  def limit(joinPS: Any, limitValue: Int): Graph[String,String] = {
    joinPS.asInstanceOf[Graph[String,String]]
  }

  def show(PS: Any): Unit = {
    val graph = PS.asInstanceOf[Graph[String,String]]
    graph.triplets.map(triplet => {
      triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr
    }).collect().foreach(println(_))
    println(s"Number of edges: ${graph.asInstanceOf[Graph[String,String]].edges.count()}")
  }

  def run(jDF: Any): Unit = {
    this.show(jDF)
  }


  def isAllDigits(x: String) = x forall Character.isDigit

  def count(joinPS: Graph[String, String]): VertexId = {
    joinPS.asInstanceOf[Graph[Array[String], String]].edges.count()
  }
}