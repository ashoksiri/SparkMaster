package com.msr.spark.streaming.hbase

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import java.sql.DriverManager
import org.apache.phoenix.jdbc.PhoenixDriver
import org.apache.phoenix.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterToHbase {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val Columns = Seq("TWEET_ID", "TWEET", "CREATED_AT")

  val filters = Array("India")

  System.setProperty("twitter4j.oauth.consumerKey", "FlRx3d0n8duIQ0UvGeGtTA")
  System.setProperty("twitter4j.oauth.consumerSecret", "DS7TTbxhmQ7oCUlDntpQQRqQllFFOiyNoOMEDD0lA")
  System.setProperty("twitter4j.oauth.accessToken", "1643982224-xTfNpLrARoWKxRh9KtFqc7aoB8KAAHkCcfC5vDk")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "PqkbuBqF3AVskgx1OKgXKOZzV7EMWRmRG0p8hvLQYKs")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      val con = DriverManager.getConnection("jdbc:phoenix:10.1.7.49")
      println(con)
      val rs = con.createStatement().execute("create table if not exists tweets("
        + "tweet_iD bigint primary key"
        + ",tweet varchar(200),created_at varchar(200))")
    } catch {
      case e: Exception => println(e.getMessage)
    }

    val stream = TwitterUtils.createStream(ssc, None, filters)

    stream.foreachRDD { rdd =>

      val comments = rdd.map(line => (line.getId, line.getText, line.getCreatedAt.toString))

      comments.take(5).foreach(println)

      comments.saveToPhoenix("TWEETS", Columns, zkUrl = Some("localhost"))
    }

    ssc.start
    ssc.awaitTermination()
  }

}