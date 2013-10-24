package com.datatorrent.demos.rabbitmq.partition;

import java.io.IOException;
import java.util.concurrent.Executors;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class HighThroughputProducer
{
  
  private static final String EXCHANGE_NAME = "hiex";
  
  static final byte[] constantMsg= new byte[1024];
  
  static {
    for (int i = 0; i < constantMsg.length; i++) {
    //define 1kb message
      constantMsg[i] = (byte) ('a' + i%26);
    }
  }

  public static void main(String[] argv) throws java.io.IOException
  {
    


    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("node10.morado.com");
    factory.setPort(5673);
    final Connection connection = factory.newConnection(Executors.newFixedThreadPool(10), new Address[]{new Address("node10.morado.com", 5673), new Address("node11.morado.com", 5673)});

    
    for (int i = 0; i < 5; i++) {
      new Thread(new Runnable() {

        @Override
        public void run()
        {
          try {

            final Channel channel = connection.createChannel();
            
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            long i = 0;
            while (true) {
              // 3 queues
              i = i++%3;
              // routing to different queue
              channel.basicPublish(EXCHANGE_NAME, "queue" + i, MessageProperties.BASIC, constantMsg);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }

        }

      }).start();
      
    }


    // channel.close();
    // connection.close();
  }

}
