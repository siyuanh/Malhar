package com.datatorrent.demos.rabbitmq.partition;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class HighThroughputConsumer
{
  private static final String EXCHANGE_NAME = "hiex";
  
  private static final String TASK_QUEUE_NAME = "hique";

  public static void main(final String[] argv) throws java.io.IOException, java.lang.InterruptedException
  {

    final ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("node10.morado.com");
    factory.setPort(5673);
    final Connection connection = factory.newConnection(Executors.newFixedThreadPool(10), new Address[]{new Address("node10.morado.com", 5673), new Address("node11.morado.com", 5673)});
    
    final AtomicLong count = new AtomicLong(0);
    
    
    TimerTask tt = new TimerTask() {
      
      @Override
      public void run()
      {
        System.out.println("Receive " + count.get() + " message/s");
        count.set(0);
      }
    };
    
    new Timer().scheduleAtFixedRate(tt, 1000, 1000);

    for (int i = 0; i < 1; i++) {
      new Thread(new Runnable() {
        
        @Override
        public void run()
        {
          
          try {
            int ackSize = 5000;
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
            channel.queueDeclare(TASK_QUEUE_NAME, false, true, true, null);
            channel.queueBind(TASK_QUEUE_NAME, EXCHANGE_NAME, "queue" + argv[0]);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
            channel.basicQos(10000);
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
            int counter = 0;
            while (true) { 
              Delivery d = consumer.nextDelivery();
//              String message = new String(delivery.getBody());

              //            System.out.println(" [x] Received '" + message + "'");
              count.addAndGet(1);
              if(++counter >= ackSize){
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


  }

}
