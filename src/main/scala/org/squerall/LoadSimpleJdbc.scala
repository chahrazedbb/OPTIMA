package org.squerall

import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD

import java.sql.{DriverManager, ResultSet}
import scala.sys.exit
// not class enclosed in an object
object LoadSimpleJdbc {
  def getResults(sc: SparkContext, columns: Array[String], options: Map[String,String]):JdbcRDD[(Int, Array[String])]=
  {
    val values = options.values.toList
    val connection = values(0)
    val driver = values(1)
    val table = values(2)
    val user = values(3)
    val password = values(4)
    val data = new JdbcRDD(sc,
      ()=>{
        Class.forName(driver).newInstance();
        DriverManager.getConnection(connection, user, password)
      }, "SELECT id, "+columns.mkString(" ,")+" FROM "+table+" WHERE ? <= id AND ID <= ?",
      lowerBound = 1, upperBound = 5000, numPartitions = 1, r=>(r.getInt(1),columns.map(c=>r.getString(c))))
      return data
  }
}