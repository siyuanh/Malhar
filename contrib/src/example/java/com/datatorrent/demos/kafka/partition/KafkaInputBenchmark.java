package com.datatorrent.demos.kafka.partition;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;

public class KafkaInputBenchmark implements StreamingApplication
{
  
  public static class CollectorModule extends BaseOperator
  {
    public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>() {

      @Override
      public void process(String arg0)
      {
      }
    };

  }
  


  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    
    dag.setAttribute(DAG.APPLICATION_NAME, "KafkaConsumerBenchmark");
    BenchmarkPartitionableKafkaInputOperator bpkio = new BenchmarkPartitionableKafkaInputOperator();
    
    Set<String> brokerSet = new HashSet<String>();
    brokerSet.add("node10.morado.com:9092");
    brokerSet.add("node11.morado.com:9092");
    brokerSet.add("node12.morado.com:9092");
    brokerSet.add("node13.morado.com:9092");
    brokerSet.add("node14.morado.com:9092");
    brokerSet.add("node15.morado.com:9092");
    brokerSet.add("node16.morado.com:9092");
    brokerSet.add("node17.morado.com:9092");
    brokerSet.add("node18.morado.com:9092");
    brokerSet.add("node19.morado.com:9092");
    
    
//     Create template high-level consumer
    Properties props = new Properties();
    props.put("zookeeper.connect", "node10.morado.com:2181");
    props.put("group.id", "main_group");
    props.put("auto.offset.reset", "smallest");
    HighlevelKafkaConsumer consumer = new HighlevelKafkaConsumer(props);
    
    
//    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(brokerSet, "benchmark", 10000, 100000, "test_kafka_autop_client", -1);
    consumer.setBrokerSet(brokerSet);
    consumer.setTopic("benchmark1");
    
    
    bpkio.setTuplesBlast(1024 * 1024);
    
    
    bpkio.setConsumer(consumer);
    
    
    bpkio = dag.addOperator("HighThroughputHighlevelKafkaConsumer", bpkio);

    CollectorModule cm = dag.addOperator("DataBlackhole", CollectorModule.class);
    dag.addStream("end", bpkio.oport, cm.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    dag.setInputPortAttribute(cm.inputPort, PortContext.PARTITION_PARALLEL, true);
    dag.setAttribute(bpkio, OperatorContext.INITIAL_PARTITION_COUNT, 1);
    
    
    

  }

}
