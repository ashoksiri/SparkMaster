package com.msr.spark.sql.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector.{SomeColumns,_}
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import scala.reflect.runtime.universe

object XmlToCassandra {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  case class employees(emp_no:Long,birth_date:String,first_name:String,last_name:String,gender:String,hire_date:String)
  val Columns = SomeColumns("emp_no","birth_date","first_name","last_name","gender","hire_date")
  
  def main(args: Array[String]): Unit =
    {
    
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
                                 .setMaster("local")
                                 .set("spark.cassandra.connection.host", "10.1.7.49")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

     CassandraConnector(sparkConf).withSessionDo { session => 
        session.execute("create table if not exists msrc.employees(emp_no bigint primary key"+
                         ",birth_date text,first_name text ,last_name text,gender text,hire_date text)") 
                         }

      val df = sqlContext.read.format("com.databricks.spark.xml")
                              .option("rootTag", "employees")
                              .option("rowTag", "employee")
                              .load("file:/home/user/Documents/dataset/employees.xml")

      //val temptable = df.withColumn("tenant_id", df("tenant_id").cast(IntegerType))
       val temptable = df.map {
                        
                 case Row(birth_date, emp_no, first_name, last_name, gender, hire_date) 
                 
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