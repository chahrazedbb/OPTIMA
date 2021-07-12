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
import scala.util.control.Breaks.{break, breakable}

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

    val stopwatch: StopWatch = new StopWatch
    stopwatch start()

    val spark = SparkSession.builder.master(sparkURI).appName("Squerall").getOrCreate
    val sc = spark.sparkContext
    var edgeIdMap : Map[String, Array[String]] = Map.empty
    var dataSource_count = 0
    var parSetId = ""
    var vertex: RDD[(VertexId, String)] = null
    var edge: RDD[Edge[String]] = null
    var finalVer: RDD[(VertexId, String)] = null
    var finalEd: RDD[Edge[String]] = null
    var header: Array[String] = null
    var FinalColumns = ""
    var finalColumns: Array[String] = null

    //for filtering
    var whereString = ""
    var nbrOfFiltersOfThisStar = 0

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

          //extracing data
          header.foreach{
            case (column) =>
              var index = header.indexOf(column)
              var head = data.first()
              var datagraph = data.filter(line => line != head)
                .map(line =>  line.split(","))

              if(vertex == null){
                vertex =  datagraph.map(line=>((edgeId+"000"+line(0)).toLong,(line(index))))
                //extracting edges
                edge = datagraph.map(line => Edge((edgeId+"000"+line(0)).toLong,(edgeId+"000"+line(0)).toLong,column))
              }else{
                vertex =  datagraph.map(line=>((edgeId+"000"+index+"000"+line(0)).toLong,(line(index))))
                //extracting edges
                edge = datagraph.map(line => Edge((edgeId+"000"+line(0)).toLong,(edgeId+"000"+index+"000"+line(0)).toLong,column))
              }
              //filtering
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
                  val wherecolumn = omitQuestionMark(star) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)

                  nbrOfFiltersOfThisStar = filters.get(value).size()

                  val conditions = filters.get(value).iterator()
                  while (conditions.hasNext) {
                    val operand_value = conditions.next()
                    whereString = column + operand_value._1 + operand_value._2

                    if(column.equals(wherecolumn)) {

                      if (operand_value._1 != "regex") {
                        if (isAllDigits(operand_value._2) && operand_value._1.equals("=")) {
                          vertex = vertex.filter {
                            case (id, prop) =>
                              ( prop.toLong == operand_value._2.toLong)
                            case _ => false
                          }
                        } else if (isAllDigits(operand_value._2) && operand_value._1.equals("<")) {
                          vertex = vertex.filter {
                            case (id, prop) => prop.toLong < operand_value._2.toLong
                            case _ => false
                          }
                        } else if (isAllDigits(operand_value._2) && operand_value._1.equals(">")) {
                          vertex = vertex.filter {
                            case (id, prop) => prop.toLong > operand_value._2.toLong
                            case _ => false
                          }
                        } else if (isAllDigits(operand_value._2) && operand_value._1.equals(">=")) {
                          vertex = vertex.filter {
                            case (id, prop) => prop.toLong >= operand_value._2.toLong
                            case _ => false
                          }
                        } else if (isAllDigits(operand_value._2) && operand_value._1.equals("<=")) {
                          vertex = vertex.filter {
                            case (id, prop) => prop.toLong <= operand_value._2.toLong
                            case _ => false
                          }
                        } else {
                          vertex = vertex.filter {
                            case (id, prop) => prop.equals(operand_value._2.split("\"")(1))
                            case _ => false
                          }
                        }
                      }
                      // else  finalGP = finalGP.filter(finalGP(column).like(operand_value._2.replace("\"","")))
                      // regular expression with _ matching an arbitrary character and % matching an arbitrary sequence
                    }
                  }
                }
              }

              if(finalVer == null) {
                finalVer = vertex
                finalEd = edge
              } else{
                finalVer = finalVer.union(vertex)
                finalEd = finalEd.union(edge)
              }
          }
          edgeIdMap = Map(str -> Array(edgeId.toString,header.mkString(",")))
        case _ =>
      }
      //in case of multiple cases
      /*if(finalVer == null) {
        finalVer = vertex
      } else{
        finalVer = finalVer.union(vertex)
      }*/
    }

    var graph:Graph[String,String] = Graph(finalVer,finalEd)

    /*val vertex2 =  graph.vertices.filter(v=>v._2 == null)
    val my2list = vertex2.map(v=>v._1.toString).collect()
    println("this is my2list")
    my2list.foreach(println(_))
    val my3list = graph.edges.filter(e => my2list.contains(e.dstId.toString)).map(e=>e.srcId.toString).collect()
    println("this is my3list")
    my3list.foreach(println(_))
    val edges2: RDD[Edge[String]] = graph.edges.filter(e => !my3list.contains(e.srcId.toString))

    graph  = Graph(graph.vertices,edges2)*/
    stopwatch stop()
    val timeTaken = stopwatch.getTime
    println(s"Time taken by query (extract+filter) method: $timeTaken")

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
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()

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
     /* if (edgeIdMap.keySet.contains(omitQuestionMark(op2)) ){
      //  id2 = edgeIdMap(omitQuestionMark(op2))(0)
      //  header2 = edgeIdMap(omitQuestionMark(op2))(1)
      }*/

      if (edgeIdMap.keySet.contains(omitQuestionMark(op1)) ){
        id1 = edgeIdMap(omitQuestionMark(op1))(0)
      //  header1 = edgeIdMap(omitQuestionMark(op1))(1)
      }

      if (firstTime) {
        firstTime = false
        seenDF.add((op1, jVal))
        seenDF.add((op2, "ID"))
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //creating the foreign key edges
        var mytemplist = graph1.edges.filter(e=>e.attr.equals(fk)).map(e=> e.dstId.toString.substring(1)).collect()

        val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1)))
          .filter((v)=>v._2!=null)
          .map{ (v) =>
            Edge((id1+"000"+v._1.toString.split("000")(2)).toLong,(id2+"000"+v._2).toLong, fk)
          }

        var finalED: RDD[Edge[String]] =  null
        edges.collect().foreach{
          e =>
            if(finalED == null){
              finalED = graph2.edges.filter(v => v.srcId == e.dstId)
            }else{
              finalED = finalED.union(graph2.edges.filter(v => v.srcId == e.dstId))
            }
        }

        jGrah = Graph(graph1.vertices.union(graph2.vertices),graph1.edges.union(finalED))

      } else {
        val dfs_only = seenDF.map(_._1)

        if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //creating the new edges
          var mytemplist = graph1.edges.filter(e=>e.attr.equals(fk)).map(e=> e.dstId.toString.substring(1)).collect()

          val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1))).filter((v)=>v._2!=null).
            map{ (v) =>
              Edge((id1+"000"+v._1.toString.split("000")(2)).toLong,(id2+"000"+v._2).toLong, fk)
            }

          var finalED: RDD[Edge[String]] = null
          edges.collect().foreach{
            e =>
              if(finalED == null){
                finalED = graph2.edges.filter(v => v.srcId == e.dstId)
              }else{
                finalED = finalED.union(graph2.edges.filter(v => v.srcId == e.dstId))
              }
          }

          jGrah = Graph(jGrah.vertices.union(graph2.vertices),jGrah.edges.union(finalED))
          seenDF.add((op2,"ID"))

        } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
          //foreign key
          val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
          //filtering the edges
          var mytemplist = graph1.edges.filter(e=>e.attr.equals(fk)).map(e=> e.dstId.toString.substring(1)).collect()

          val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1)))
            .filter((v)=>v._2!=null)
            .map{ (v) =>
              Edge((id1+"000"+v._1.toString.split("000")(2)).toLong,(id2+"000"+v._2).toLong, fk)
            }

          var finalED: RDD[Edge[String]] = null
          edges.collect().foreach{
            e =>
              if(finalED == null){
                finalED =  jGrah.edges.filter(v => v.srcId == e.dstId)
              }else{
                finalED = finalED.union(jGrah.edges.filter(v => v.srcId == e.dstId))
              }
          }

          jGrah = Graph(jGrah.vertices.union(graph1.vertices),graph1.edges.union(finalED))
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
        //creating the new edges
        println("this is fk " + fk)
        val tempEdges: RDD[Edge[String]] =graph1.edges.filter(e=>e.attr.equals(fk))
        //filtering the edges
        var mytemplist : mutable.MutableList[String]=  mutable.MutableList()
        tempEdges.collect().foreach{
          case e =>
            if(!mytemplist.contains(e.dstId.toString.substring(1))){
              mytemplist += e.dstId.toString.substring(1)
            }
        }
        val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1))).map{ (v) =>
          Edge((id1+"00"+v._1.toString.substring(4)).toLong,(id2+"00"+v._2).toLong, fk)
        }

        var finalED: RDD[Edge[String]] = null
        edges.collect().foreach{
          e =>
            if(finalED == null){
              finalED = graph2.edges.filter(v => v.srcId == e.dstId)
            }else{
              finalED = finalED.union(graph2.edges.filter(v => v.srcId == e.dstId))
            }
        }

        jGrah = Graph(jGrah.vertices.union(graph2.vertices).filter((v)=>v._2!=null), jGrah.edges.union(finalED))
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

      } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
        //foreign key
        val fk = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        //creating the edges
        val tempEdges: RDD[Edge[String]] =graph1.edges.filter(e=>e.attr.equals(fk))
        //filtering the edges
        var mytemplist : mutable.MutableList[String]=  mutable.MutableList()
        tempEdges.collect().foreach{
          case e =>
            if(!mytemplist.contains(e.dstId.toString)){
              mytemplist += e.dstId.toString.substring(1)
            }
        }
        val edges: RDD[Edge[String]] = graph1.vertices.filter(v=>mytemplist.contains(v._1.toString.substring(1))).map{ (v) =>
          Edge((id1+"00"+v._1.toString.substring(4)).toLong,(id2+"00"+v._2).toLong, fk)
        }

        var finalED: RDD[Edge[String]] = null
        edges.collect().foreach{
          e =>
            if(finalED == null){
              finalED =  jGrah.edges.filter(v => v.srcId == e.dstId)
            }else{
              finalED = finalED.union(jGrah.edges.filter(v => v.srcId == e.dstId))
            }
        }

        jGrah = Graph(jGrah.vertices.union(graph1.vertices).filter((v)=>v._2!=null),
          graph1.edges.union(finalED))
        jGrah = jGrah.subgraph(vpred = (vid,vd)=>vd!=null)

        println("third joining")
        jGrah.edges.collect().foreach(println(_))

      } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
        println("hi there 33")

        pendingJoins.enqueue((op1, (op2, jVal)))
      }
      pendingJoins = pendingJoins.tail
    }
    stopwatch stop()
    val timeTaken = stopwatch.getTime
    println(s"Time taken by join method: $timeTaken")

    jGrah
  }

  def project(jDF: Any, columnNames: Seq[String],  edgeIdMap: Map[String,Array[String]],distinct: Boolean): Graph[String, String] = {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    var jGP = jDF.asInstanceOf[Graph[String,String]]
    var edges: RDD[Edge[String]] = null

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

    val vertex = jGP.vertices.filter(v=>v._2 == null)
    val my2list = vertex.map(v=>v._1.toString).collect()
    println("this is my2list")
    my2list.foreach(println(_))
    val my3list = edges.filter(e => my2list.contains(e.dstId.toString)).map(e=>e.srcId.toString).collect()
    println("this is my3list")
    my3list.foreach(println(_))
    edges = edges.filter(e => !my3list.contains(e.srcId.toString))

    if(distinct){
      jGP = Graph(
        jGP.vertices,
        edges.distinct()
      )
    }else{
      jGP = Graph(
        jGP.vertices,
        edges
      )
    }

    stopwatch stop()
    val timeTaken = stopwatch.getTime
    println(s"Time taken by projection method: $timeTaken")

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
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    val graph = PS.asInstanceOf[Graph[String,String]]
    graph.triplets.map(triplet => {
      triplet.srcId + ", " + triplet.srcAttr + " is the " + triplet.attr + " of " +  triplet.dstId + ", " + triplet.dstAttr
    }).collect().foreach(println(_))
    println(s"Number of edges: ${graph.asInstanceOf[Graph[String,String]].edges.count()}")

    stopwatch stop()
    val timeTaken = stopwatch.getTime
    println(s"Time taken by show method: $timeTaken")
  }

  def run(jDF: Any): Unit = {
    this.show(jDF)
  }


  def isAllDigits(x: String) = x forall Character.isDigit

  def count(joinPS: Graph[String, String]): VertexId = {
    joinPS.asInstanceOf[Graph[Array[String], String]].edges.count()
  }
}