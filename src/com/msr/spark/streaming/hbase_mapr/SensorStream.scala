package com.msr.spark.streaming.hbase_mapr

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel


object SensorStream extends Serializable {

  // schema for sensor data   
  case class Sensor(resid: String, date: String, 
                    time: String, hz: Double, 
                    disp: Double, flo: Double, 
                    sedPPM: Double, psi: Double, 
                    chlPPM: Double) extends Serializable
  
   //time outs
  val timeout = 50 // Terminate after N seconds
  val batchSeconds = 5 // Size of batch intervals

                    
  //kafka Configurations
  val (zkQuorum, group) = ("localhost:2181", "kelly")
  
  // create a StreamingContext, the main entry point for all streaming functionality
  val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]").set("spark.files.overwrite", "true")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(batchSeconds))
  
  // function to parse line of sensor data into Sensor class
  def parseSensor(str: String): Sensor = {
    val p = str.split(",")
   
    Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
    
  }


  def main(args: Array[String]): Unit = {
    
    // parse the lines of data into sensor objects
    val textDStream = KafkaUtils.createStream(ssc, zkQuorum, group, Map("hadoop"->2), StorageLevel.MEMORY_ONLY).map(_._2)
    textDStream.print()
    val sensorDStream = textDStream.map(parseSensor)
    
    sensorDStream.foreachRDD{ 
      rdd =>
      // There exists at least one element in RDD
      if (!rdd.partitions.isEmpty) {
        // filter sensor data for low psi
        val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
        println("low pressure alert ")
        alertRDD.take(2).foreach(println)
        alertRDD.saveAsTextFile("/home/user/ashok/alertout")

      }
    }

    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination();

  }

}