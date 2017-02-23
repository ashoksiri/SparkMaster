package com.msr.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import it.nerdammer.spark.hbase._

object HbaseTest {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
    //sparkConf.set("spark.hbase.host", "10.1.10.212")
    sparkConf.set("hbase.zookeeper.quorum", "node1.hdp.com");
    sparkConf.set("hbase.zookeeper.property.clientPort", "2181");
    sparkConf.set("zookeeper.znode.parent", "/hbase-unsecure");
    val sc = new SparkContext(sparkConf)
    
   val rdd =  sc.hbaseTable[(String,String)]("sensor").select("chlPPM", "flo").inColumnFamily("data");
    rdd.foreach(println)
    
  }
}