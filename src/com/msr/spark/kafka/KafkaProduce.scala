package com.msr.spark.kafka

import java.io.File
import kafka.javaapi.producer.Producer
import java.util.Properties
import kafka.producer.ProducerConfig
import java.io.FileReader
import java.io.BufferedReader
import kafka.producer.KeyedMessage
import scala.io.Source

object KafkaProduce {
  
   var producer:Producer[Int, String]= _ 
   val props:Properties = new Properties
  
  def setProperties(broker:String)
  {
      props.put("metadata.broker.list", broker);
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			this.producer = new Producer[Int, String](new ProducerConfig(props));
			
  }
  
  
  def produce(broker:String,topic:String,file:File){
    setProperties(broker)
    val reader = new BufferedReader(new FileReader(file))
    val fileContents = Source.fromFile(file).getLines()
    
    fileContents.foreach { x => 
    
       var data =new KeyedMessage[Int, String](topic, x)
       this.producer.send(data)
    }
    
    this.producer.close
  }
  
}