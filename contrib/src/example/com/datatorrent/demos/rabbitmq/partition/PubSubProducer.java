package com.datatorrent.demos.rabbitmq.partition;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class PubSubProducer
{

  private static final String EXCHANGE_NAME = "logs";

  public static void main(String[] argv)
                throws java.io.IOException {

      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");
      Connection connection = factory.newConnection();
      Channel channel = connection.createChannel();

      channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
      String queueName1 = channel.queueDeclare("queue1", true, false, false, null).getQueue();
      String queueName2 = channel.queueDeclare("queue2", true, false, false, null).getQueue();
      System.out.println(queueName1);
      System.out.println(queueName2);

      String option = "";
      while (true && !option.equals("exit")) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        option = reader.readLine();
        channel.basicPublish(EXCHANGE_NAME, "", null, option.getBytes());
        System.out.println(" [x] Sent '" + option + "'");
      }

      channel.close();
      connection.close();
  }

}
