package com.datatorrent.demos.rabbitmq.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.PartitionableOperator;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class BenchmarkPartitionableRabbitMQOutputOperator extends BaseOperator implements PartitionableOperator,ActivationListener<Context>
{

  
  //define 1kb message
  static final byte[] constantMsg= new byte[1024];
  
  static {
    for (int i = 0; i < constantMsg.length; i++) {
      constantMsg[i] = (byte) ('a' + i%26);
    }
  }
  
  private int partitionNumber = 5;
  
  private String exchangeName = "hiex";
  
  private String hostname = "node10.morado.com";
  
  private int port = 5673;

  private int queueNumber = 10;
  
  @Override
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int arg1)
  {
    List<Partition<?>> returnPartitions = new ArrayList<PartitionableOperator.Partition<?>>(partitionNumber);
    
    Partition<BenchmarkPartitionableRabbitMQOutputOperator> templatePartition = (Partition<BenchmarkPartitionableRabbitMQOutputOperator>) partitions.iterator().next();
    for (int i = 0; i < partitionNumber; i++) {
      Partition<BenchmarkPartitionableRabbitMQOutputOperator> newPartition = templatePartition.getInstance(new BenchmarkPartitionableRabbitMQOutputOperator());
      returnPartitions.add(newPartition);
    }
    return returnPartitions;
  }
  @Override
  public void activate(Context arg0)
  {
    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(hostname);
      factory.setPort(port);
      final Connection connection = factory.newConnection(Executors.newFixedThreadPool(10), new Address[]{new Address("node10.morado.com", 5673), new Address("node11.morado.com", 5673)});
      
      for (int i = 0; i < 5; i++) {
        new Thread(new Runnable() {

          @Override
          public void run()
          {
            try {

              final Channel channel = connection.createChannel();
              
              channel.exchangeDeclare(exchangeName, "direct");

              long i = 0;
              while (true) {
                // 3 queues
                i = i++%queueNumber;
                // routing to different queue
                channel.basicPublish(exchangeName, "queue" + i, MessageProperties.BASIC, constantMsg);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }).start();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  @Override
  public void deactivate()
  {
    
  } 
  
  
  public void setPartitionNumber(int partitionNumber)
  {
    this.partitionNumber = partitionNumber;
  }
  
  public int getPartitionNumber()
  {
    return partitionNumber;
  }
  
  
  public int getPort()
  {
    return port;
  }
  public void setPort(int port)
  {
    this.port = port;
  }
  
  
  public String getHostname()
  {
    return hostname;
  }
  public void setHostname(String hostname)
  {
    this.hostname = hostname;
  }
  
  
  public int getQueueNumber()
  {
    return queueNumber;
  }
  public void setQueueNumber(int queueNumber)
  {
    this.queueNumber = queueNumber;
  }

}
