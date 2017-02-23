package com.msr.spark.sql.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._


object CassandraRead {
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  conf.set("spark.cassandra.connection.host","10.25.3.131")
  
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    
    
    val emp = sc.cassandraTable("msrc","employees_100k");
    
    emp.take(5).foreach(println)
  }
}