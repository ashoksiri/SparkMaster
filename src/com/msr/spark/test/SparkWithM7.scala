package com.msr.spark.test

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.msr.spark.kafka.KafkaProduce

object SparkWithM7 {
  
  def main(args: Array[String]) {
    //Initiate spark context with spark master URL. You can modify the URL per your environment. 
    val sc = new SparkContext("local", "M7Test")
    val tableName = "sensor"
    val ssc = new StreamingContext(sc,Seconds(5))
    
    // Add local HBase conf
   val conf = HBaseConfiguration.create()
   conf.set("hbase.zookeeper.quorum", "localhost");
   //conf.set("hbase.zookeeper.quorum", "node1.hdp.com");
   //conf.set("hbase.zookeeper.property.clientPort", "2181");
   //conf.set("zookeeper.znode.parent", "/hbase-unsecure");
   conf.set(TableInputFormat.INPUT_TABLE, tableName)

    // create m7 table with column family
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tableName)) {
      print("Creating M7 Table")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("data".getBytes()));
      tableDesc.addFamily(new HColumnDescriptor("alert".getBytes()));
      admin.createTable(tableDesc)
    }

   val (zkQuorum,groupid) = ("localhost:2181","kelly")
   
   val lines = KafkaUtils.createStream(ssc, zkQuorum, groupid, Map("hadoop"->2),StorageLevel.MEMORY_ONLY).map(_._2)
   val sensorDStream = lines.map(HBaseStreamSensorData.parseSensor)
    sensorDStream.foreachRDD { rdd =>
      //put data into table
      
       KafkaProduce.produce("localhost:9092","hadoop", new java.io.File("/home/user/Documents/dataset/stream/sensor"))
      
     
      rdd.foreach { sensor =>

        val myTable = new HTable(conf, tableName);
          var put = new Put(new String(sensor.resid + "_" + sensor.date + " " + sensor.time).getBytes());

          if (sensor.psi < 5.0) {
            put.add("alert".getBytes, "alert:psi".getBytes, sensor.psi.toString().getBytes)
          } else {
            put.add("data".getBytes, "data:hz".getBytes, sensor.hz.toString().getBytes)
            put.add("data".getBytes, "data:disp".getBytes, sensor.disp.toString().getBytes)
            put.add("data".getBytes, "data:flo".getBytes, sensor.flo.toString().getBytes)
            put.add("data".getBytes, "data:sedPPM".getBytes, sensor.sedPPM.toString().getBytes)
            put.add("data".getBytes, "data:psi".getBytes, sensor.psi.toString().getBytes)
            put.add("data".getBytes, "data:chlPPM".getBytes, sensor.chlPPM.toString().getBytes)
          }

          myTable.put(put);
        myTable.flushCommits();
      }
   }
	
	  ssc.start
	  ssc.awaitTermination()
    
  }
}