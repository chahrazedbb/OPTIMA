package org.squerall

import com.google.common.collect.ArrayListMultimap
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait QueryExecutorGraph[T] { // T is a ParSet (Parallel dataSet)

  /* Generates a ParSet with the number of filters (on predicates) in the star */
  def query(sources : mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])],
            optionsMap: mutable.HashMap[String, (Map[String, String],String)],
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
           ) : (T, Integer, String, Map[String, Array[String]],Any)

  /* Transforms a ParSet to another ParSet based on the SPARQL TRANSFORM clause */
  def transform(ps: Any, column: String, transformationsArray : Array[String]): Any

  /* Print the schema of the ParSet */
  def join(joins: ArrayListMultimap[String, (String, String)],
           prefixes: Map[String, String],
           star_df: Map[String,T],
           edgeIdMap: Map[String,Array[String]],
           sc: Any): T

  /* Generates a new ParSet projecting out one or more attributes */
  def project(jDF: Any, columnNames: Seq[String], distinct: Boolean): T

  /* Counts the number of tuples of a ParSet */
  def count(joinPS: T): Long

  /* Sort tuples of a ParSet based on an attribute variable */
  def orderBy(joinPS: Any, direction: String, variable: String, sc: Any): T

  /* Group attributes based on aggregates function(s) */
  def groupBy(joinPS: Any, groupBys: (ListBuffer[String], mutable.Set[(String,String)])): T

  /* Return the first 'limitValue' values of the ParSet */
  def limit(joinPS: Any, limitValue: Int) : T

  /* Show some results */
  def show(PS: Any)

  /* Compute the results */
  def run(jDF: Any)
}
