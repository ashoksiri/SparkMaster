package com.msr.spark.sql.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector.{SomeColumns,_}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.reflect.runtime.universe

object CsvToCassandra {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  case class employees(emp_no:Long,birth_date:String,first_name:String,last_name:String,gender:String,hire_date:String)
  val Columns = SomeColumns("emp_no","birth_date","first_name","last_name","gender","hire_date")
  
  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "10.1.7.49")
      //.set("spark.eventLog.enabled", "true")
      
      CassandraConnector(conf).withSessionDo { session => 
        session.execute("create table if not exists msrc.employees(emp_no bigint primary key"+
                         ",birth_date text,first_name text ,last_name text,gender text,hire_date text)") 
                         }

    val sc = new SparkContext(conf)
    val csv = "file:/home/user/Documents/dataset/employees_db/employees.csv"
   
    var table = sc.textFile(csv).map(line => line.replace("\"", ""))
                                .map(line => line.split(","))
                                .map(columns => employees(columns(0).toLong,columns(1),columns(2),columns(3),columns(4),columns(5)))
    
                                
    table.saveToCassandra("msrc", "employees",Columns )
    
    sc.stop()
    
    println("job completed")
  }
}