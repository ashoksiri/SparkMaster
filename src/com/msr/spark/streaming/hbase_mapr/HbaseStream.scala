package com.msr.spark.streaming.hbase_mapr

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import com.msr.spark.kafka.KafkaProduce
import java.io.File
import org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper
import kafka.serializer.StringDecoder


object HbaseStream extends Serializable{

  final val tableName = "sensor"
  final val cfDataBytes = Bytes.toBytes("data")
  final val cfAlertBytes = Bytes.toBytes("alert")
  final val colHzBytes = Bytes.toBytes("hz")
  final val colDispBytes = Bytes.toBytes("disp")
  final val colFloBytes = Bytes.toBytes("flo")
  final val colSedBytes = Bytes.toBytes("sedPPM")
  final val colPsiBytes = Bytes.toBytes("psi")
  final val colChlBytes = Bytes.toBytes("chlPPM")

  case class Sensor(resid: String, date: String, 
                    time: String, hz: Double, 
                    disp: Double, flo: Double, 
                    sedPPM: Double, psi: Double, 
                    chlPPM: Double) extends Serializable
  
   def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), 
             p(3).toDouble, p(4).toDouble, 
             p(5).toDouble, p(6).toDouble, 
             p(7).toDouble, p(8).toDouble)
    }                 
   
 
    def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      val rowkey = sensor.resid + "_" + dateTime
      val put = new Put(Bytes.toBytes(rowkey))
 
      // add to column family data, column  data values to put object 
      put.add(cfDataBytes, colHzBytes, Bytes.toBytes(sensor.hz))
      put.add(cfDataBytes, colDispBytes, Bytes.toBytes(sensor.disp))
      put.add(cfDataBytes, colFloBytes, Bytes.toBytes(sensor.flo))
      put.add(cfDataBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM))
      put.add(cfDataBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      put.add(cfDataBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      
    }
  
   def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val key = sensor.resid + "_" + dateTime
      val p = new Put(Bytes.toBytes(key))
      // add to column family alert, column psi data value to put object 
      p.add(cfAlertBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
    }
  
  val (zkQuorum, group) = ("localhost:2181", "kelly")
  val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]").set("spark.files.overwrite", "true")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))
  
  def main(args: Array[String]): Unit = {
    
     val conf = HBaseConfiguration.create()
         conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
         conf.set("hbase.zookeeper.quorum", "node1.hdp.com");
         conf.set("hbase.zookeeper.property.clientPort", "2181");
         conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    
     val jobConfig: JobConf = new JobConf(conf, this.getClass)
         jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/home/user/ashok/out")
         jobConfig.setOutputFormat(classOf[TableOutputFormat])
         jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    
         println("set configuration")
       
         val lines = KafkaUtils.createStream(ssc, zkQuorum, group, Map("hadoop"->2)).map(_._2)
         
         //val topicsSet = "hadoop".split(",").toSet
         //val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
         //KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
         
          // parse the lines of data into sensor objects  maprfs:///
          val sensorDStream = lines.map(HbaseStream.parseSensor)

          sensorDStream.print()
     
           sensorDStream.foreachRDD 
           { 
             rdd =>
             
               KafkaProduce.produce("localhost:9092", "hadoop", new File("/home/user/Documents/dataset/stream/sensor"))
               
             // filter sensor data for low psi
           val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
           alertRDD.take(1).foreach(println)
      
           // convert sensor data to put object and write to HBase table column family data
           rdd.map(HbaseStream.convertToPut).saveAsHadoopDataset(jobConfig)
           // convert alert data to put object and write to HBase table column family alert
           alertRDD.map(HbaseStream.convertToPutAlert).saveAsHadoopDataset(jobConfig)
          }
          
     ssc.start()
    println("start streaming")
    // Wait for the computation to terminate
    ssc.awaitTermination()
    
  }
}