package com.msr.spark.sql.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector.{SomeColumns,_}
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.reflect.runtime.universe

object JsonToCassandra {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
 
  case class employees(emp_no:Long,birth_date:String,first_name:String,last_name:String,gender:String,hire_date:String)
  
  val Columns = SomeColumns("emp_no","birth_date","first_name","last_name","gender","hire_date")
  
  def main (args: Array[String]) {

    val conf = new SparkConf(true)
                    .setAppName(this.getClass.getName)
                    .setMaster("local")
                    .set("spark.cassandra.connection.host", "10.1.7.49")
                    //.set("spark.eventLog.enabled", "true")
       
        CassandraConnector(conf).withSessionDo { session => 
        session.execute("create table if not exists msrc.employees(emp_no bigint primary key"+
                         ",birth_date text,first_name text ,last_name text,gender text,hire_date text)") 
                         }
     val sc = new SparkContext(conf)
     val hc = new SQLContext(sc)
    
     val file = "file:/home/user/Documents/dataset/employees.json"
    
     val empdf = hc.read.json(file) 
     
     val temptable = empdf.map {
                        
                 case Row(birth_date,emp_no , first_name, last_name, gender, hire_date) 
                 
                 =>
                
                employees(emp_no.asInstanceOf[Long], 
                birth_date.asInstanceOf[String], 
                first_name.asInstanceOf[String], 
                last_name.asInstanceOf[String], 
                gender.asInstanceOf[String], 
                hire_date.asInstanceOf[String])
        }

      temptable.saveToCassandra("msrc", "employees", Columns)

      sc.stop
      println("Job Completed")
     
    }
}