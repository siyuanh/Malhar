package com.datatorrent.demos.rabbitmq.partition;

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
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class BenchmarkPartitionableRabbitMQInputOperator extends BaseOperator implements ActivationListener<Context>, PartitionableOperator
{
  
  private static final String EXCHANGE_NAME = "hiex";
  
  private int queueIndex = 0;
  
  private int partitionNumber = 10;

  @Override
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity)
  {
    List<Partition<?>> returnPartitions = new ArrayList<PartitionableOperator.Partition<?>>(partitionNumber);
    
    Partition<BenchmarkPartitionableRabbitMQInputOperator> templatePartition = (Partition<BenchmarkPartitionableRabbitMQInputOperator>) partitions.iterator().next();
    for (int i = 0; i < partitionNumber; i++) {
      BenchmarkPartitionableRabbitMQInputOperator bpio = new BenchmarkPartitionableRabbitMQInputOperator();
      bpio.setQueueIndex(i);
      Partition<BenchmarkPartitionableRabbitMQInputOperator> newPartition = templatePartition.getInstance(bpio);
      returnPartitions.add(newPartition);
    }
    return returnPartitions;
  }

  @Override
  public void activate(Context ctx)
  {
    
    try {
      final ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("node10.morado.com");
      factory.setPort(5673);
      final Connection connection = factory.newConnection(Executors.newFixedThreadPool(10), new Address[] { new Address("node10.morado.com", 5673), new Address("node11.morado.com", 5673) });

      for (int i = 0; i < 1; i++) {
        new Thread(new Runnable() {

          @Override
          public void run()
          {

            try {
              int ackSize = 5000;
              Channel channel = connection.createChannel();
              channel.exchangeDeclare(EXCHANGE_NAME, "direct");
              String queueName = channel.queueDeclare().getQueue();
              channel.queueBind(queueName, EXCHANGE_NAME, "queue" + queueIndex);
              System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
              channel.basicQos(10000);
              QueueingConsumer consumer = new QueueingConsumer(channel);
              channel.basicConsume(queueName, false, consumer);
              int counter = 0;
              while (true) {
                Delivery d = consumer.nextDelivery();
                if (++counter >= ackSize) {
                  counter = 0;
                  channel.basicAck(d.getEnvelope().getDeliveryTag(), true);
                }
                //            System.out.println(" [x] Done");
              }
            } catch (Exception e) {
            }

          }
        }).start();
      }
    } catch (Exception e) {
      // TODO: handle exception
    }

  }

  @Override
  public void deactivate()
  {
    
  }

  public int getPartitionNumber()
  {
    return partitionNumber;
  }

  public void setPartitionNumber(int partitionNumber)
  {
    this.partitionNumber = partitionNumber;
  }
  
  public void setQueueIndex(int queueIndex)
  {
    this.queueIndex = queueIndex;
  }
  
  public int getQueueIndex()
  {
    return queueIndex;
  }

}
