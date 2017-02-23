package com.msr.spark.test

import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamTest {
  
  import org.apache.spark._
  import org.apache.spark.streaming._
  
  //Logger.getLogger("org").setLevel(Level.OFF)
  //Logger.getLogger("org").setLevel(Level.OFF)
  
  val (zkQuorum, group, topics, numThreads) = ("localhost:2181", "kelly", "trainee", "2")
  val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(2))
  
  def main(args: Array[String]): Unit = {
    
      
  //val stream=    ssc.textFileStream("file:/home/user/Documents/dataset/stream")
  
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, Map("hadoop"->2)).map(_._2)
    
    //val stream = ssc.socketTextStream("localhost", 7744, StorageLevel.MEMORY_ONLY)
  
  
  lines.print()
  
  ssc.start()
  ssc.awaitTermination()
  }
}