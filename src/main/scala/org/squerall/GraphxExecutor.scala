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

class GraphxExecutor (sparkURI: String, mappingsFile: String) extends QueryExecutor[Graph[String,String]] {
  def query(sources : mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])],
            optionsMap_entity: mutable.HashMap[String, (Map[String, String],String)],
            toJoinWith: Boolean,
            star: String,
            prefixes: Map[String, String],
            select: util.List[String],
            star_predicate_var: mutable.HashMap[(String, String), String],
            neededPredicates: mutable.Set[String],
            filters: ArrayListMultimap[String, (String, String)],
            leftJoinTransformations: (String, Array[String]),
            rightJoinTransformations: Array[String],
            joinPairs: Map[(String,String), String],
            edgeId:Int
           ): (Graph[String, String], Integer, String,  Map[String, Int], Any) = {

    val spark = SparkSession.builder.master(sparkURI).appName("Squerall").getOrCreate
    val sc = spark.sparkContext

    var finalGP : Graph[String,String] = null
    var edgeIdMap : Map[String, Int] = Map.empty
    var dataSource_count = 0
    var parSetId = ""

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

      var vertex: RDD[(VertexId, String)] = null
      var edge: RDD[Edge[String]] = null
      var df : DataFrame = null

      sourceType match {
        case "csv" => df = spark.read.options(options).csv(sourcePath)

          val mycolumns = columns.split(",")
          var myFinalColumns = df.columns
          val toRemove = "`".toSet

          df.columns.zipWithIndex.foreach {
            case (x, i) =>
              mycolumns.foreach(col =>
                if(col.split("AS")(0).filterNot(toRemove).trim.equals(x))
                {
                  myFinalColumns(i) = col.split("AS")(1).filterNot(toRemove).trim
                }
              )
          }

          println("columns columns" )
          myFinalColumns.foreach(println(_))

          edgeIdMap = Map(str -> edgeId)

          df.columns.zipWithIndex.foreach{ case(col,i) =>
            if(vertex == null){
              vertex = df.rdd.map((x)=>((edgeId.toString + edgeId.toString + edgeId.toString + x.get(0).toString).toLong, x.get(i).toString))
              edge = df.rdd.map((x)=>(Edge((edgeId.toString + edgeId.toString + edgeId.toString + x.get(0).toString).toLong,
                (edgeId.toString + edgeId.toString + edgeId.toString + x.get(0).toString).toLong, myFinalColumns.toList(i))))
            }else if(edge == null){
              vertex = vertex.union(df.rdd.map((x)=>((edgeId.toString + i + x.get(0).toString).toLong,
                x.get(i).toString)))
              edge = df.rdd.map((x)=>(Edge((edgeId.toString + edgeId.toString + edgeId.toString + x.get(0).toString).toLong,
                (edgeId.toString + i + x.get(0).toString).toLong, myFinalColumns.toList(i))))
            }else{
              vertex = vertex.union(df.rdd.map((x)=>((edgeId.toString + i + x.get(0).toString).toLong,
                x.get(i).toString)))
              edge = edge.union(df.rdd.map((x)=>(Edge((edgeId.toString + edgeId.toString + edgeId.toString + x.get(0).toString).toLong,
                (edgeId.toString + i + x.get(0).toString).toLong, myFinalColumns.toList(i)))))
            }
          }
          finalGP = Graph(vertex, edge)
          println("tttttttttttttttttttttttttttttt")
          val facts: RDD[String] = finalGP.triplets.map(triplet => triplet.srcAttr + ","+triplet.attr+"," + triplet.dstAttr)
          facts.collect.foreach(println(_))
          println("those are the vertices *******************************")
          vertex.collect.foreach(println(_))
          println("those are the edges *******************************")
          edge.collect.foreach(println(_))

        case _ =>
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

          var edge: RDD[Edge[String]] = null
          var tempedge: RDD[Edge[String]] = null
          var vertex: RDD[(VertexId, String)] = null

          val operand_value = conditions.next()

          whereString = column + operand_value._1 + operand_value._2

          println("whereString : " + whereString)

          if (operand_value._1 != "regex") {
              edge = finalGP.edges.filter{case Edge(_,_,label) => label.equals(column)}
              println("this is edges wherestring")
              edge.collect.foreach(println(_))
              if(edge != null) {
                if(isAllDigits(operand_value._2)  && operand_value._1.equals("=")){
                  println("this is ==" + isAllDigits(operand_value._2) + " " )
                  edge.collect.foreach(e =>
                    if (vertex == null) {
                      vertex = finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong==operand_value._2.toLong)
                        case _ => false
                      }
                    } else {
                      vertex = vertex.union(finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong==operand_value._2.toLong)
                        case _ => false
                      })
                    }
                  )
                }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<")){
                  edge.collect.foreach(e =>
                    if (vertex == null) {
                      vertex = finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong<operand_value._2.toLong)
                        case _ => false
                      }
                    } else {
                      vertex = vertex.union(finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong<operand_value._2.toLong)
                        case _ => false
                      })
                    }
                  )
                }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">")){
                  edge.collect.foreach(e =>
                    if (vertex == null) {
                      vertex = finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong>operand_value._2.toLong)
                        case _ => false
                      }
                    } else {
                      vertex = vertex.union(finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong>operand_value._2.toLong)
                        case _ => false
                      })
                    }
                  )
                }else if(isAllDigits(operand_value._2)  && operand_value._1.equals(">=")){
                  edge.collect.foreach(e =>
                    if (vertex == null) {
                      vertex = finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong >= operand_value._2.toLong)
                        case _ => false
                      }
                    } else {
                      vertex = vertex.union(finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong >= operand_value._2.toLong)
                        case _ => false
                      })
                    }
                  )
                }else if(isAllDigits(operand_value._2)  && operand_value._1.equals("<=")){
                  edge.collect.foreach(e =>
                    if (vertex == null) {
                      vertex = finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong <= operand_value._2.toLong)
                        case _ => false
                      }
                    } else {
                      vertex = vertex.union(finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.toLong <= operand_value._2.toLong)
                        case _ => false
                      })
                    }
                  )
                }else{
                  println("this is  wherestring wherestring wherestring wherestring wherestring" + operand_value._2.split("\"")(1))
                  edge.collect.foreach(e =>
                    if (vertex == null) {
                      vertex = finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.equals(operand_value._2.split("\"")(1)))
                        case _ => false
                      }
                    } else {
                      vertex = vertex.union(finalGP.vertices.filter {
                        case (id, prop) => (id == e.dstId && prop.equals(operand_value._2.split("\"")(1)))
                        case _ => false
                      })
                    }
                  )
                }
                println("this is vertices wherestring")
                if (vertex != null) {
                  vertex.collect.foreach(println(_))
                  vertex.collect.foreach {
                    case (id, prop) =>
                      if (tempedge == null) {
                        tempedge = edge.filter { case Edge(_, dis, _) => dis == id }
                      } else {
                        tempedge = tempedge.union(edge.filter { case Edge(_, dis, _) => dis == id })
                      }
                  }
                  edge = null
                  if(tempedge != null){
                    tempedge.collect.foreach(e =>
                      if (edge == null) {
                        edge = finalGP.edges.filter { case Edge(src, _, _) => src == e.srcId }
                      } else {
                        edge = edge.union(finalGP.edges.filter { case Edge(src, _, _) => src == e.srcId })
                      }
                    )
                    if(edge!=null){
                      finalGP = Graph(finalGP.vertices, edge)

                    }else{
                      val sc = spark.sparkContext
                      vertex = sc.parallelize(Array((0L,"")))
                      edge = sc.parallelize( Array.empty[Edge[String]])
                      finalGP = Graph(vertex,edge)
                    }
                  }else{
                    val sc = spark.sparkContext
                    vertex = sc.parallelize(Array((0L,"")))
                    edge = sc.parallelize( Array.empty[Edge[String]])
                    finalGP = Graph(vertex,edge)
                  }
                }else{
                  val sc = spark.sparkContext
                  vertex = sc.parallelize(Array((0L,"")))
                  edge = sc.parallelize( Array.empty[Edge[String]])
                  finalGP = Graph(vertex,edge)
                }

              }else{
                println("no filter here ")
                finalGP.edges.collect.foreach(println(_))
              }

          }
          // else  finalGP = finalGP.filter(finalGP(column).like(operand_value._2.replace("\"","")))
          // regular expression with _ matching an arbitrary character and % matching an arbitrary sequence
        }
      }
    }

    val facts: RDD[String] = finalGP.triplets.map(triplet =>triplet.srcAttr+ " "  +triplet.attr+ " " + triplet.dstAttr)
    println("this is finalGP")
    facts.collect.foreach(println(_))

    (finalGP, nbrOfFiltersOfThisStar, parSetId, edgeIdMap, sc)
  }

  def transform(ps: Any, column: String, transformationsArray: Array[String]): Any = {
    ps.asInstanceOf[Graph[String,String]]
  }

  def join(joins: ArrayListMultimap[String, (String, String)],
           prefixes: Map[String, String],
           star_df: Map[String, Graph[String, String]],
           edgeIdMap: Map[String,Int],
           sc: Any)
  :Graph[String, String] = {
    import scala.collection.JavaConversions._
    import scala.collection.mutable.ListBuffer

    var pendingJoins = mutable.Queue[(String, (String, String))]()
    val seenDF : ListBuffer[(String,String)] = ListBuffer()
    var firstTime = true
    var jGrah :Graph[String,String] = null

    val scc = sc.asInstanceOf[SparkContext]

    val it = joins.entries.iterator
    while ({it.hasNext}) {
      val entry = it.next

      val op1 = entry.getKey
      val op2 = entry.getValue._1
      val jVal = entry.getValue._2

      val gph1 = star_df(op1)
      val gph2 = star_df(op2)

      val njVal = get_NS_predicate(jVal)
      val ns = prefixes(njVal._1)

      var edges : RDD[Edge[String]] = null
      var tmpgph : Graph[String,String] = null
      var edge44: RDD[Edge[String]] = null
      var id : String = ""
      var attList = new ListBuffer[String]()

      it.remove()

      //getting the added number to the edges ids
      if (edgeIdMap.keySet.contains(omitQuestionMark(op2)) ){
        id = edgeIdMap(omitQuestionMark(op2)).toString
        println("val2 " + id)
      }

      if (firstTime) {
        print("this is join 1")
        val stopwatch: StopWatch = new StopWatch
        stopwatch start()
        firstTime = false
        seenDF.add((op1, jVal))
        seenDF.add((op2, "ID"))

        val f1: RDD[String] = gph1.triplets.map(triplet =>triplet.srcAttr + " " + triplet.attr + " " + triplet.dstAttr)
        val f2: RDD[String] = gph2.triplets.map(triplet => triplet.srcAttr + " " + triplet.attr + " " + triplet.dstAttr)

        //extracting the foreign key
        edges = gph1.edges.filter {
          case Edge(_, _, label) => label.equals(omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns)
        }

        //puting the value into sequence
        tmpgph = Graph(gph1.vertices,edges)
        val facts: RDD[String] = tmpgph.triplets.map(triplet => triplet.dstAttr)
        //filtring the related edges from the second grah
        for(att <- facts.collect){
          if(edge44 == null){
            edge44 = gph2.edges.filter{case Edge(src,dis,_) => src.toString.equals (id + id + id +att)}
            attList += att
          }else{
            if(!attList.contains(att)) {
              edge44 = edge44.union(gph2.edges.filter { case Edge(src, dis, _) => src.toString.equals ( id + id + id +att) })
              attList += att
            }
          }
        }
        if(edge44 == null || edge44.count()==0){
          val vertex: RDD[(VertexId,String)] = scc.parallelize(Array((0L,"")))
          edge44 = scc.parallelize( Array.empty[Edge[String]])
          jGrah = Graph(vertex,edge44)
        }else{
          println("this is edges gph2")
          edge44.collect.foreach(println(_))
          jGrah = Graph(gph1.vertices.union(gph2.vertices), gph1.edges.union(edge44))
        }

        stopwatch stop()
        val timeTaken = stopwatch.getTime

        println("time aken by join 1 = " + timeTaken)
      } else {
        val dfs_only = seenDF.map(_._1)

        if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {

          print("this is join 2")
          val stopwatch: StopWatch = new StopWatch
          stopwatch start()
          //extracting the foreign key
          edges = jGrah.edges.filter {
            case Edge(_, _, label) => label.equals(omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns)
          }

          //puting the value into sequence
          tmpgph = Graph(jGrah.vertices,edges)
          val facts: RDD[String] = tmpgph.triplets.map(triplet => triplet.dstAttr)

          edges = null
          //filtring the related edges from the second grah
          for(att <- facts.collect){
            println("2) this is the id + id + id +att" + id + id + id +att)

            if(edges == null){
              edges = gph2.edges.filter{case Edge(src,dis,_) => src.toString.equals(id + id + id +att)}
              attList += att
            }else{
              if(!attList.contains(att)) {
                edges = edges.union(gph2.edges.filter { case Edge(src, dis, _) => src.toString.equals(id + id + id + att) })
                attList += att
              }
            }
          }

          //creating the new graph
          if(edges != null && edges.count()!=0){
          jGrah = Graph(jGrah.vertices.union(gph2.vertices),jGrah.edges.union(edges))
          }else{
            val vertex: RDD[(VertexId,String)] = scc.parallelize(Array((0L,"")))
            val edge: RDD[Edge[String]] = scc.parallelize(Array(Edge(0L,0L,"")))
            jGrah = Graph(vertex,edge)
          }
          //creating the new graph
          println("this is the second graph")

          val facts20: RDD[String] = jGrah.triplets.map(triplet =>
            triplet.srcAttr+ "," + triplet.attr + ","+ triplet.dstAttr)
          facts20.collect.foreach(println(_))


          seenDF.add((op2,"ID"))

          println("number of edges join 2 = "  + jGrah.edges.count())
          stopwatch stop()

         // val timeTaken = stopwatch.getTime

        //  println("time aken by join 3 = " + timeTaken)

        } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
          print("this is join 3")
          val stopwatch: StopWatch = new StopWatch
          stopwatch start()
          /* val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                              val rightJVar = omitQuestionMark(op2) + "_ID"
                              jDF = df1.join(jDF, df1.col(leftJVar).equalTo(jDF.col(rightJVar)))
          */
          //extracting the foreign key
          edges = gph1.edges.filter {
            case Edge(_, _, label) => label.equals(omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns)
          }

          //puting the value into sequence
          tmpgph = Graph(gph1.vertices,edges)
          val facts: RDD[String] = tmpgph.triplets.map(triplet => triplet.dstAttr)

          edges = null
          //filtring the related edges from the second grah
          for(att <- facts.collect){
            println("3) this is the id + id + id +att" + id + id + id +att)
            if(edges == null){
              edges = jGrah.edges.filter{case Edge(src,dis,_) => src.toString.equals(id + id + id +att)}
              attList += att
            }else{
              if(!attList.contains(att)) {
                edges = edges.union(jGrah.edges.filter { case Edge(src, dis, _) => src.toString.equals(id + id + id + att) })
                attList += att
              }
            }
          }

          //creating the new graph
          if(edges != null && edges.count()!=0){
          jGrah = Graph(gph1.vertices.union(jGrah.vertices), gph1.edges.union(edges))
          }else{
            val vertex: RDD[(VertexId,String)] = scc.parallelize(Array((0L,"")))
            val edge: RDD[Edge[String]] = scc.parallelize(Array(Edge(0L,0L,"")))
            jGrah = Graph(vertex,edge)
          }
          seenDF.add((op1,jVal))

          println("number of edges join 3 = "  + jGrah.edges.count())
          stopwatch stop()

         // val timeTaken = stopwatch.getTime

         // println("time aken by join 3 = " + timeTaken)

        } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
          print("this is join 4")

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

      val njVal = get_NS_predicate(jVal)
      val ns = prefixes(njVal._1)

      val gph1 = star_df(op1)
      val gph2 = star_df(op2)

      var edges : RDD[Edge[String]] = null
      var tmpgph : Graph[String,String] = null
      var id : String = ""
      var attList = new ListBuffer[String]()

      //getting the added number to the edges ids
      if (edgeIdMap.keySet.contains(omitQuestionMark(op2)) ){
        id = edgeIdMap(omitQuestionMark(op2)).toString
        println("val2 " + id)
      }

      if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {

        print("this is join 5")

        //extracting the foreign key
        edges = jGrah.edges.filter {
          case Edge(_, _, label) => label.equals(omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns)
        }

        //puting the value into sequence
        tmpgph = Graph(jGrah.vertices,edges)
        val facts: RDD[String] = tmpgph.triplets.map(triplet => triplet.dstAttr)

        println("this is the map" + edgeIdMap)

        edges = null
        //filtring the related edges from the second grah
        for(att <- facts.collect){
          println("4) this is the id + id + id +att" + id + id + id +att)
          if(edges == null){
            edges = gph2.edges.filter{case Edge(src,dis,_) => src.toString.equals(id + id + id +att)}
            attList += att
          }else{
            if(!attList.contains(att)) {
              edges = edges.union(gph2.edges.filter { case Edge(src, dis, _) => src.toString.equals(id + id + id + att) })
              attList += att
            }
          }
        }

        if(edges != null && edges.count()!=0 ){
        jGrah = Graph(jGrah.vertices.union(gph2.vertices), jGrah.edges.union(edges))
        }else{
          val vertex: RDD[(VertexId,String)] = scc.parallelize(Array((0L,"")))
          val edge: RDD[Edge[String]] = scc.parallelize(Array(Edge(0L,0L,"")))
          jGrah = Graph(vertex,edge)
        }
        seenDF.add((op2,"ID"))
      } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
        print("this is join 6")

        //extracting the foreign key
        edges = jGrah.edges.filter {
          case Edge(_, _, label) => label.equals(omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns)
        }

        //puting the value into sequence
        tmpgph = Graph(jGrah.vertices,edges)
        val facts: RDD[String] = tmpgph.triplets.map(triplet => triplet.dstAttr)

        println("this is the map" + edgeIdMap)

        edges = null
        //filtring the related edges from the second grah
        for(att <- facts.collect){
          println("5) this is the id + id + id +att" + id + id + id +att)
          if(edges == null){
            edges = gph1.edges.filter{case Edge(src,dis,_) => src.toString.equals(id + id + id +att)}
            attList += att
          }else{
            if(!attList.contains(att)) {
              edges = edges.union(gph1.edges.filter { case Edge(src, dis, _) => src.toString.equals(id + id + id + att) })
              attList += att
            }
          }
        }

        if(edges != null && edges.count()!=0){

          jGrah = Graph(jGrah.vertices.union(gph1.vertices), jGrah.edges.union(edges))
        }else{
          val vertex: RDD[(VertexId,String)] = scc.parallelize(Array((0L,"")))
          val edge: RDD[Edge[String]] = scc.parallelize(Array(Edge(0L,0L,"")))
          jGrah = Graph(vertex,edge)
        }
        seenDF.add((op1,jVal))
      } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
        print("this is join 7")

        pendingJoins.enqueue((op1, (op2, jVal)))
      }

      pendingJoins = pendingJoins.tail
    }

    jGrah
  }

  def project(jDF: Any, columnNames: Seq[String], distinct: Boolean): Graph[String, String] = {
    val stopwatch: StopWatch = new StopWatch
    stopwatch start()
    var jGP = jDF.asInstanceOf[Graph[String,String]]
    var myEdges: RDD[Edge[String]] = null

    println("cccccccccccccccccccccccccccccc")
    columnNames.foreach(println(_))

    for(name <- columnNames){
      if(myEdges == null){
        myEdges = jGP.edges.filter {
          case Edge(_, _, label) => label.equals(name)
        }
      }else{
        myEdges = myEdges.union(jGP.edges.filter {
          case Edge(_, _, label) => label.equals(name)
        })
      }
    }

    jGP = Graph(
      jGP.vertices,
      myEdges
    )

   // stopwatch stop()
   // val timeTaken = stopwatch.getTime
    //println("this is projection time = " + timeTaken)

    jGP
  }

  def count(joinPS: Graph[String, String]): Long = {
    joinPS.asInstanceOf[Graph[String,String]].edges.count()
  }

  def orderBy(joinPS: Any, direction: String, variable: String, sc: Any): Graph[String, String] = {
    val scc = sc.asInstanceOf[SparkContext]
    var joinPsVertices: RDD[(VertexId, String)] = null
    var vertex: RDD[(VertexId, String)] = null
    var edge: RDD[Edge[String]] = null
    var graph: Graph[String,String] = null
    var verticesList = new ListBuffer[(VertexId,VertexId,String)]()
    var edgesList = new ListBuffer[(VertexId,VertexId,String)]()

    //extracting edges where variable is equal to column name
    val joinPsEdges =  joinPS.asInstanceOf[Graph[String,String]].edges.filter(e=>e.attr.equals(variable))

    if(joinPsEdges != null){
      //extracting respective vertices
      joinPsEdges.collect.foreach(e=>
        //val joinPsVertices = joinPS.asInstanceOf[Graph[String,String]].vertices.filter(v=>)
        if(joinPsVertices==null){
          joinPsVertices = joinPS.asInstanceOf[Graph[String,String]].vertices.filter(v=> v._1 == e.dstId || v._1 == e.srcId)
        }else{
          joinPsVertices = joinPsVertices.union(joinPS.asInstanceOf[Graph[String,String]].vertices.
            filter(v=> v._1 == e.dstId || v._1 == e.srcId))
        }
      )
    }

    if(joinPsVertices != null){
      var mynum: VertexId = 0L
      joinPsVertices = joinPsVertices.sortBy(_._2).union(joinPS.asInstanceOf[Graph[String,String]].vertices)

      var alreadyIn = new ListBuffer[(VertexId,String)]

      println("rrrrrrrrrrrrrr")
      joinPsVertices.collect.foreach{
        v =>
          if(!alreadyIn.contains(v)){
            verticesList.append((mynum,v._1,v._2))
            mynum = mynum + 1
            alreadyIn.append(v)
          }
      }
      for(n<-verticesList){println(n)}

      var src: VertexId = 0L
      var dis: VertexId = 0L
      joinPS.asInstanceOf[Graph[String,String]].edges.collect.foreach{e=>
        for(v<-verticesList){
          if(e.srcId==v._2){
            src = v._1
          }
        }
        for(v<-verticesList) {
          if(e.dstId==v._2){
            dis = v._1
          }
        }
        edgesList.append((src,dis,e.attr))
      }

      for (v<-verticesList){
        if(vertex==null){
          vertex = scc.parallelize(Array((v._1,v._3)))
        }else{
          vertex = vertex.union(scc.parallelize(Array((v._1,v._3))))
        }
      }
      for (v<-edgesList){
        if(edge==null){
          edge = scc.parallelize(Array(Edge(v._1,v._2,v._3)))
        }else{
          edge = edge.union(scc.parallelize(Array(Edge(v._1,v._2,v._3))))
        }
      }

      graph = Graph(vertex,edge.sortBy(_.dstId))

    }else{
      graph = joinPS.asInstanceOf[Graph[String,String]]
    }

    graph
  }

  def groupBy(joinPS: Any, groupBys: (ListBuffer[String], mutable.Set[(String, String)])): Graph[String, String] = {
    joinPS.asInstanceOf[Graph[String,String]]
  }

  def limit(joinPS: Any, limitValue: Int): Graph[String, String] = {
    joinPS.asInstanceOf[Graph[String,String]]
    /*    var graph = Graph(
      joinPS.asInstanceOf[Graph[String,String]].vertices.top(limitValue),
        joinPS.asInstanceOf[Graph[String,String]].edges
    )
    graph*/
  }

  def show(PS: Any): Unit = {
    val graph = PS.asInstanceOf[Graph[String,String]]

    println("this is it")

    val facts2: RDD[String] = graph.triplets.map(triplet =>
      triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)
    facts2.collect.foreach(println(_))

    println(s"Number of edges: ${graph.asInstanceOf[Graph[String,String]].edges.count()}")
  }

  def run(jDF: Any): Unit = {
    this.show(jDF)
  }

/*  def compareValues(opd1: String,opd2: String, op: String): Boolean = {
    var cond : Boolean=false

    if(isAllDigits(opd1) && isAllDigits(opd2)){
      val  a1: Long = opd1.toLong
      val  a2: Long = opd2.toLong
      cond =  compareNumberValues(a1,a2,op)
    }else{
      cond = compareStringValues(opd1, opd2, op)
    }
    cond
  }

  def compareStringValues(opd1: String,opd2: String, op: String): Boolean = {
    op match {
      case "<" => opd1 < opd2
      case ">" => opd1 > opd2
      case "=" => opd1.equals(opd2)
      case "<=" => opd1 <= opd2
      case ">=" => opd1 >= opd2
      case _ => false
    }
  }

  def compareNumberValues(opd1: Long,opd2: Long, op: String): Boolean = {
    op match {
      case "<" => opd1 < opd2
      case ">" => opd1 > opd2
      case "=" => opd1 == opd2
      case "<=" => opd1 <= opd2
      case ">=" => opd1 >= opd2
      case _ => false
    }
  }
*/
def isAllDigits(x: String) = x forall Character.isDigit

}
