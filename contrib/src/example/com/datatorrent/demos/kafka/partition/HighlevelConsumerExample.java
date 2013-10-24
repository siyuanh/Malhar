package com.datatorrent.demos.kafka.partition;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
 
public class HighlevelConsumerExample {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
 
    public HighlevelConsumerExample(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }
 
    public void run() {
      new Thread(new Runnable() {
        
        @Override
        public void run()
        {
          while(true){
            consumer.commitOffsets();
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
          
        }
      });
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        while (it.hasNext()){
            System.out.println("Consumes message: " + new String(it.next().message()));
        }
 
    }
 
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("consumer.id", "consumer1");
        props.put("auto.commit.enable", "false");
//        props.put("zookeeper.session.timeout.ms", "400");
//        props.put("zookeeper.sync.time.ms", "200");
//        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
 
    public static void main(String[] args) {
      
//      SimpleConsumer consumer = new SimpleConsumer("localhost", 9092, 100000, 64 * 1024, "leaderLookup");
//      List<String> topics = new ArrayList<String>();
//      topics.add("test_topic");
//      TopicMetadataRequest req = new TopicMetadataRequest(topics);
//      kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
//
//      List<kafka.javaapi.TopicMetadata> metaData = resp.topicsMetadata();
//      for (kafka.javaapi.TopicMetadata item : metaData) {
//        for (final PartitionMetadata part : item.partitionsMetadata()) {
//          System.out.println(part);
//        }
//      }

//      Properties props = new Properties();
//      props.put("zookeeper.connect", "localhost:2182");
//      props.put("group.id", "group1");
//      
//      HighlevelKafkaConsumer consumer = new HighlevelKafkaConsumer(props);
//      consumer.setTopic("test_topic");
//      
//      consumer.create();
//      consumer.start();
      
 
        HighlevelConsumerExample example = new HighlevelConsumerExample("localhost:2182", "group1", "test_topic");
        example.run();
 
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
 
        }
        example.shutdown();
        
        

    }
}