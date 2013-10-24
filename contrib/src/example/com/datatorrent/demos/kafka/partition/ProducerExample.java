package com.datatorrent.demos.kafka.partition;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerExample implements Runnable
{
  public static void main(String[] args) throws InterruptedException
  {
    new Thread(new ProducerExample()).start();
  }

  private final Random rand =  new Random();
  @Override
  public void run()
  {
    
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "localhost:9092, localhost:9093, localhost:9094");
//    props.put("metadata.broker.list", "localhost:9092");
    props.setProperty("partitioner.class", KafkaTestPartitioner.class.getCanonicalName());
    props.setProperty("producer.type", "sync");
    props.setProperty("topic.metadata.refresh.interval.ms", "10000");
    
    Producer<String, String>producer = new Producer<String, String>(new ProducerConfig(props));
    
    while (true) {
      String messageStr = "Message_" + System.currentTimeMillis() + "_";
      int k = rand.nextInt(100);
      producer.send(new KeyedMessage<String, String>("test_topic", "" + k, messageStr + k));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
  }
}