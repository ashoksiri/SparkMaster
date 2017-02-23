package com.msr.spark.sql.hbase

import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HColumnDescriptor
import java.util.Date
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object CSVToHbase {
  
  
  val sc = new SparkContext("local",this.getClass.getName)
  
  val table = "employees"
  
  val data = "data".getBytes
  val emp_no = "emp_no".getBytes
  val birth_date ="birth_date".getBytes
  val first_name = "first_name".getBytes
  val last_name ="last_name".getBytes
  val gender ="gender".getBytes
  val hire_date ="hire_date".getBytes
  
  case class employees(emp_no:Long,birth_date:
      String,first_name:String,last_name:String,
      gender:String,hire_date:String)
  
  def parseLine(line:String):employees={
    
    val columns = line.split(",")
    employees(columns(0).toLong,columns(1),columns(2),columns(3),columns(4),columns(5))
  }
  
  val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    conf.set("hbase.zookeeper.quorum", "node1.hdp.com");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("zookeeper.znode.parent", "/hbase-unsecure");
  
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/home/user/ashok/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, table)
  
  
  def main(args: Array[String]): Unit = {
    
    val csv = "file:/home/user/Documents/dataset/employees_db/employees.csv"
    
    val rdd = sc.textFile(csv).map(parseLine)
    
    
    val admin = new HBaseAdmin(conf);

    if (!admin.isTableAvailable(table)) {
      println("Creating  Table "+table)
      val tableDesc = new HTableDescriptor(table)
      tableDesc.addFamily(new HColumnDescriptor(data));
      admin.createTable(tableDesc)
    }
    
  val putObject =   rdd.map{
      employee =>
      val rowkey = new Date().getTime
      val put = new Put(rowkey.toString.getBytes)
      // add to column family data, column  data values to put object 
          put.add(data, emp_no, employee.emp_no.toString().getBytes)
          put.add(data, birth_date, employee.birth_date.toString().getBytes)
          put.add(data, first_name, employee.first_name.toString().getBytes)
          put.add(data, last_name, employee.last_name.toString().getBytes)
          put.add(data, gender, employee.gender.toString().getBytes)
          put.add(data, hire_date, employee.hire_date.toString().getBytes)
          (new ImmutableBytesWritable(rowkey.toString.getBytes),put)
    }
  
  putObject.saveAsHadoopDataset(jobConfig)
  
  }
  
}