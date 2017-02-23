package com.msr.spark.streaming.hbase_mapr

import scala.reflect.runtime.universe

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadHbaseTable {
  
  //case class for mapping Hbase Table
  case class Sensor(key:String ,resID: String, date: String, 
                    hz: Double,disp: Double, flo: Double, 
                    sedPPM: Double, psi: Double,chlPPM: Double) extends Serializable
  
  //Spark Configuration
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
                                 .setMaster("local[4]")
                                  .set("spark.files.overwrite", "true")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  
  import sqlContext.implicits._
 
  // Hbase Table Name to Read
  val tableName = "sensor"
  
  // Hbase Configuration to Connect remote Hbase
  val conf = HBaseConfiguration.create()
         conf.set("hbase.zookeeper.quorum", "node1.hdp.com");
         conf.set("hbase.zookeeper.property.clientPort", "2181");
         conf.set("zookeeper.znode.parent", "/hbase-unsecure");
         conf.set(TableInputFormat.INPUT_TABLE,tableName)
  
  def main(args: Array[String]): Unit = {
      
           //Spark Mapreduce type Key Class and Value Class to read Hbase table as Dataset
           val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
                                        classOf[ImmutableBytesWritable], 
                                        classOf[org.apache.hadoop.hbase.client.Result])
           
           val hbasedf = rdd.map(x => parseCells(x._2.rawCells())).toDF
           
           hbasedf.show
          
           
  }
  
 // user defined function to parse Hbase cells to Sensor Object        
 def parseCells(row :Array[Cell]):Sensor={
  
          var key :String =""
          var resID: String = ""
          var date: String = ""
          var hz: Double = 0
          var disp: Double = 0
          var flo: Double = 0
          var sedPPM: Double = 0
          var psi: Double = 0
          var chlPPM: Double = 0
          
           for(x <- row)
           {
             key = new String(x.getRow)
              new String(CellUtil.cloneQualifier(x)) match {
               case "resID" => resID = new String(CellUtil.cloneValue(x))
               case "date"  => date  = new String(CellUtil.cloneValue(x))
               case "hz"    => hz    = new String(CellUtil.cloneValue(x)).toDouble
               case "disp"  => disp  = new String(CellUtil.cloneValue(x)).toDouble
               case "flo"   => flo   = new String(CellUtil.cloneValue(x)).toDouble
               case "sedPPM"=> sedPPM= new String(CellUtil.cloneValue(x)).toDouble
               case "psi"   => psi   = new String(CellUtil.cloneValue(x)).toDouble
               case "chlPPM"=> chlPPM= new String(CellUtil.cloneValue(x)).toDouble
             }
           }
        Sensor(key,resID,date,hz,disp,flo,sedPPM,psi,chlPPM)
       }
}