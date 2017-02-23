package com.msr.spark.streaming.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.sql.SQLContext

object FlumeToHbase {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

    case class vmstat(R:String, B:String, SWPD:String, 
                    FREE:String, BUFF:String, CACH:String, 
                    SI:String, SO:String, BI:String, BO:String, 
                    INS:String, CS:String, US:String, 
                    SY:String, ID:String, WA:String,
                    DATE:String,TIME:String)
     
    final val cfDataBytes = "data".getBytes
                    
     def parseVmstat(str: String): vmstat = {
      val c = str.split("[\\s]+")
      var obj:vmstat = null
      
      try
      {
        obj = vmstat(c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12), c(13), c(14), c(15), c(16),c(18),c(19))
      }
      catch{
        case e: ArrayIndexOutOfBoundsException => 
      }
     obj 
    }                

  def convertToPut(vmstat: vmstat): (ImmutableBytesWritable, Put) = {
    
      val rowkey = vmstat.DATE + " " + vmstat.TIME
      // create a composite row key: sensorid_date time
      val put = new Put(rowkey.toString.getBytes)
      // add to column family data, column  data values to put object 
          put.add(cfDataBytes, "R".getBytes, vmstat.R.toString().getBytes)
          put.add(cfDataBytes, "B".getBytes, vmstat.B.toString().getBytes)
          put.add(cfDataBytes, "SWPD".getBytes, vmstat.SWPD.toString().getBytes)
          put.add(cfDataBytes, "FREE".getBytes, vmstat.FREE.toString().getBytes)
          put.add(cfDataBytes, "BUFF".getBytes, vmstat.BUFF.toString().getBytes)
          put.add(cfDataBytes, "CACH".getBytes, vmstat.CACH.toString().getBytes)
          put.add(cfDataBytes, "SI".getBytes, vmstat.SI.toString().getBytes)
          put.add(cfDataBytes, "SO".getBytes, vmstat.SO.toString().getBytes)
          put.add(cfDataBytes, "BI".getBytes, vmstat.BI.toString().getBytes)
          put.add(cfDataBytes, "BO".getBytes, vmstat.BO.toString().getBytes)
          put.add(cfDataBytes, "INS".getBytes, vmstat.INS.toString().getBytes)
          put.add(cfDataBytes, "CS".getBytes, vmstat.CS.toString().getBytes)
          put.add(cfDataBytes, "US".getBytes, vmstat.US.toString().getBytes)
          put.add(cfDataBytes, "SY".getBytes, vmstat.SY.toString().getBytes)
          put.add(cfDataBytes, "ID".getBytes, vmstat.ID.toString().getBytes)
          put.add(cfDataBytes, "WA".getBytes, vmstat.WA.toString().getBytes)
          
      return (new ImmutableBytesWritable(rowkey.toString.getBytes), put)
    }
  
  
  
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    
    val tableName = "vmstat"                
    
  def main(args: Array[String]): Unit = {

      // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("hbase.zookeeper.quorum", "node1.hdp.com");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    
    val admin = new HBaseAdmin(conf);

    if (!admin.isTableAvailable(tableName)) {
      println("Creating  Table "+tableName)
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("data".getBytes()));
      admin.createTable(tableDesc)
    }
    
      val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/home/user/ashok/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    println("set configuration")
    
    
    val stream = FlumeUtils.createPollingStream(ssc, "localhost", 6666, StorageLevel.MEMORY_ONLY_SER_2)
    stream.foreachRDD {
      sparkFlumeEvent =>

        val data = sparkFlumeEvent.map(line => line.event)

        var rdd = data.map(AvroFlunmeEvent => parseVmstat(new String(AvroFlunmeEvent.getBody.array()))).filter { x => x!=null }
        rdd.toDF.show
        rdd.map(convertToPut).saveAsHadoopDataset(jobConfig);
     
    }

    ssc.start
    ssc.awaitTermination
  }
}