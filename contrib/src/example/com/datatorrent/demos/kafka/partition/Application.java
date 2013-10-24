package com.datatorrent.demos.kafka.partition;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
import com.datatorrent.contrib.kafka.PartitionableKafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.lib.io.ConsoleOutputOperator;

public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    
    PartitionableKafkaSinglePortStringInputOperator kinput = dag.addOperator("p_kafka_input", new PartitionableKafkaSinglePortStringInputOperator());
    
    
    Set<String> brokerSet = new HashSet<String>();
    brokerSet.add("localhost:9092");
    brokerSet.add("localhost:9093");
    
    SimpleKafkaConsumer skc = new SimpleKafkaConsumer(brokerSet, "test_topic", 10000, 100000, "test_kafka_autop_client", -1);
    skc.setBrokerSet(brokerSet);
    skc.setTopic("test_topic");
    
    
//    Properties props = new Properties();
//    props.put("zookeeper.connect", "localhost:2182");
//    props.put("group.id", "main_group");
//    props.put("auto.offset.reset", "smallest");
//    props.put("auto.commit.enable", "false");
//    HighlevelKafkaConsumer hkc = new HighlevelKafkaConsumer(props);
//    hkc.setBrokerSet(brokerSet);
//    hkc.setTopic("test_topic");
    


    kinput.setConsumer(skc);
    
    // Set the 
    dag.setAttribute(kinput, OperatorContext.INITIAL_PARTITION_COUNT, 1);
    
    ConsoleOutputOperator coutput = dag.addOperator("console", new ConsoleOutputOperator());
    
    dag.addStream("kafkapartitionstream", kinput.outputPort, coutput.input);

  }
  

}
