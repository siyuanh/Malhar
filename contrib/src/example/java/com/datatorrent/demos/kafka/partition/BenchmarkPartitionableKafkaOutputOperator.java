package com.datatorrent.demos.kafka.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.PartitionableOperator;
import com.yammer.metrics.Metrics;

@ShipContainingJars(classes={kafka.javaapi.consumer.SimpleConsumer.class, org.I0Itec.zkclient.ZkClient.class, scala.ScalaObject.class, Metrics.class})
public class BenchmarkPartitionableKafkaOutputOperator implements PartitionableOperator, InputOperator, ActivationListener<OperatorContext>
{

  //define 1kb message
  static final byte[] constantMsg= new byte[1024];
  
  static {
    for (int i = 0; i < constantMsg.length; i++) {
      constantMsg[i] = (byte) ('a' + i%26);
    }
  }
  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void emitTuples()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int partitionNum)
  {
    
    Collection<Partition<?>> newPartitions = new ArrayList<PartitionableOperator.Partition<?>>(10);
    //get first one as tempalte
    @SuppressWarnings("unchecked")
    Partition<BenchmarkPartitionableKafkaOutputOperator> template = (Partition<BenchmarkPartitionableKafkaOutputOperator>) partitions.iterator().next();
    for (int i = 0; i < 10; i++) {
      BenchmarkPartitionableKafkaOutputOperator bpkoo = new BenchmarkPartitionableKafkaOutputOperator();
      Partition<BenchmarkPartitionableKafkaOutputOperator> p = template.getInstance(bpkoo);
      newPartitions.add(p);
    }
    return newPartitions;
  }

  @Override
  public void activate(OperatorContext arg0)
  {
    
    new Thread(new Runnable() {
      @Override
      public void run()
      {
        Properties props = new Properties();
        props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "node10.morado.com:9092, node11.morado.com:9092, node12.morado.com:9092,"
            + " node13.morado.com:9092, node14.morado.com:9092, node15.morado.com:9092, "
            + "node16.morado.com:9092, node17.morado.com:9092, node18.morado.com:9092, "
            + "node19.morado.com:9092");
//        props.put("metadata.broker.list", "localhost:9092");
        props.setProperty("partitioner.class", KafkaTestPartitioner.class.getCanonicalName());
        props.setProperty("producer.type", "async");
        props.setProperty("topic.metadata.refresh.interval.ms", "10000");
        
        Producer<String, String>producer = new Producer<String, String>(new ProducerConfig(props));
        long k = 0;
        while (true) {
          producer.send(new KeyedMessage<String, String>("benchmark1", "" + (k++), new String(constantMsg)));
//          try {
//            Thread.sleep(1000);
//          } catch (InterruptedException e) {
//            e.printStackTrace();
//          }
        }
      }
    }).start();
    

    
  }

  @Override
  public void deactivate()
  {
    
  }

}
