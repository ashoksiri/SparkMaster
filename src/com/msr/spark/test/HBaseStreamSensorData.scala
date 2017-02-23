package com.msr.spark.test

import org.apache.kafka.common.metrics.Sensor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import com.msr.spark.kafka.KafkaProduce

object HBaseStreamSensorData {
  
  
  final val tableName = "sensor"
  final val cfDataBytes = "data".toString().getBytes
  final val cfAlertBytes = "alert".toString().getBytes
  final val colresIDBytes = "resid".toString().getBytes
  final val coldateBytes = "date".toString().getBytes
  final val colHzBytes = "hz".toString().getBytes
  final val colDispBytes = "disp".toString().getBytes
  final val colFloBytes = "flo".toString().getBytes
  final val colSedBytes = "sedPPM".toString().getBytes
  final val colPsiBytes = "psi".toString().getBytes
  final val colChlBytes = "chlPPM".toString().getBytes
  
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
  
  val (zkQuorum, group) = ("localhost:2181", "kelly")
  val sparkConf = new SparkConf().setAppName("KafkaWordCount")
                                 .setMaster("local[4]")
                                  .set("spark.files.overwrite", "true")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))
  
  val conf = HBaseConfiguration.create()
         conf.set("hbase.zookeeper.quorum", "node1.hdp.com");
         conf.set("hbase.zookeeper.property.clientPort", "2181");
         conf.set("zookeeper.znode.parent", "/hbase-unsecure");
  
  val htable:HTable = new HTable(conf, tableName);
  
  def main(args: Array[String]): Unit = {
    
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, Map("hadoop"->2)).map(_._2)
    val sensorDStream = lines.map(HBaseStreamSensorData.parseSensor)
    sensorDStream.print

    /*sensorDStream.foreachRDD { rdd =>

      rdd.foreach { sensor =>

        val dateTime = sensor.date + " " + sensor.time
        val rowkey = sensor.resid + "_" + dateTime
        val put = new Put(rowkey.getBytes)
        if (sensor.psi < 5.0) {
          put.add(cfAlertBytes, colPsiBytes, sensor.psi.toString().getBytes)
          put.add(cfDataBytes, colresIDBytes, sensor.resid.toString().getBytes)
          put.add(cfDataBytes, coldateBytes, sensor.date.toString().getBytes)
          put.add(cfDataBytes, colHzBytes, sensor.hz.toString().getBytes)
          put.add(cfDataBytes, colDispBytes, sensor.disp.toString().getBytes)
          put.add(cfDataBytes, colFloBytes, sensor.flo.toString().getBytes)
          put.add(cfDataBytes, colSedBytes, sensor.sedPPM.toString().getBytes)
          put.add(cfDataBytes, colPsiBytes, sensor.psi.toString().getBytes)
          put.add(cfDataBytes, colChlBytes, sensor.chlPPM.toString().getBytes)
        } else {
          put.add(cfDataBytes, colresIDBytes, sensor.resid.toString().getBytes)
          put.add(cfDataBytes, coldateBytes, sensor.date.toString().getBytes)
          put.add(cfDataBytes, colHzBytes, sensor.hz.toString().getBytes)
          put.add(cfDataBytes, colDispBytes, sensor.disp.toString().getBytes)
          put.add(cfDataBytes, colFloBytes, sensor.flo.toString().getBytes)
          put.add(cfDataBytes, colSedBytes, sensor.sedPPM.toString().getBytes)
          put.add(cfDataBytes, colPsiBytes, sensor.psi.toString().getBytes)
          put.add(cfDataBytes, colChlBytes, sensor.chlPPM.toString().getBytes)
        }

        htable.put(put)
        htable.flushCommits();
      }
    }*/
  ssc.start
  ssc.awaitTermination()  
  }
  
}