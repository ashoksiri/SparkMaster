package com.msr.spark.streaming.hbase_mapr

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel

object HBaseSensorStream extends Serializable {

  final val tableName     = "sensor"
  final val cfDataBytes   = "data".getBytes
  final val cfAlertBytes  = "alert".getBytes
  final val colresIDBytes = "resID".toString().getBytes
  final val coldateBytes = "date".toString().getBytes
  final val colHzBytes = "hz".getBytes
  final val colDispBytes = "disp".getBytes
  final val colFloBytes = "flo".getBytes
  final val colSedBytes = "sedPPM".getBytes
  final val colPsiBytes = "psi".getBytes
  final val colChlBytes = "chlPPM".getBytes

  // schema for sensor data   
  case class Sensor(resid: String, date: String, 
                    time: String, hz: Double, 
                    disp: Double, flo: Double, 
                    sedPPM: Double, psi: Double, 
                    chlPPM: Double) extends Serializable
                    
  def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
    }               

  def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val rowkey = sensor.resid + "_" + dateTime
      val put = new Put(rowkey.toString.getBytes)
      // add to column family data, column  data values to put object 
          put.add(cfDataBytes, colresIDBytes, sensor.resid.toString().getBytes)
          put.add(cfDataBytes, coldateBytes, sensor.date.toString().getBytes)
          put.add(cfDataBytes, colHzBytes, sensor.hz.toString().getBytes)
          put.add(cfDataBytes, colDispBytes, sensor.disp.toString().getBytes)
          put.add(cfDataBytes, colFloBytes, sensor.flo.toString().getBytes)
          put.add(cfDataBytes, colSedBytes, sensor.sedPPM.toString().getBytes)
          put.add(cfDataBytes, colPsiBytes, sensor.psi.toString().getBytes)
          put.add(cfDataBytes, colChlBytes, sensor.chlPPM.toString().getBytes)
      return (new ImmutableBytesWritable(rowkey.toString.getBytes), put)
    }
  def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val key = sensor.resid + "_" + dateTime
      val p = new Put(key.toString.getBytes)
      // add to column family alert, column psi data value to put object 
      p.add(cfAlertBytes, colPsiBytes, sensor.psi.toString.getBytes)
      return (new ImmutableBytesWritable(key.toString.getBytes), p)
    }
  
    
    val (zkQuorum, group) = ("localhost:2181", "kelly")
    val topic = "hadoop".split(",").map(x => (x ,2)).toMap
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
                                   .setMaster("local[4]")
                                   .set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
 
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("hbase.zookeeper.quorum", "node1.hdp.com");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    
  def main(args: Array[String]): Unit = {
    

    val admin = new HBaseAdmin(conf);

    if (!admin.isTableAvailable(tableName)) {
      println("Creating  Table "+tableName)
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("data".getBytes()));
      tableDesc.addFamily(new HColumnDescriptor("alert".getBytes()));
      admin.createTable(tableDesc)
    }

    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/home/user/ashok/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    println("set configuration")
    
    val sensorDStream = KafkaUtils.createStream(ssc, zkQuorum, group, topic, StorageLevel.MEMORY_ONLY)
      .map(_._2).map(parseSensor)

    sensorDStream.print()

    sensorDStream.foreachRDD { rdd =>
      
      // filter sensor data for low psi
      val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
      alertRDD.take(1).foreach(println)
      
      // convert sensor data to put object and write to HBase table column family data
      rdd.map(convertToPut).saveAsHadoopDataset(jobConfig)
      
      // convert alert data to put object and write to HBase table column family alert
      alertRDD.map(convertToPutAlert).saveAsHadoopDataset(jobConfig)
    }
    // Start the computation
    ssc.start()
    println("start streaming")
    // Wait for the computation to terminat
    ssc.awaitTermination()

  }

}