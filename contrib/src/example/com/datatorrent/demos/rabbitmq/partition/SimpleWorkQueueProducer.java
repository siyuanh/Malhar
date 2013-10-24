package com.datatorrent.demos.rabbitmq.partition;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class SimpleWorkQueueProducer
{
  private static final String TASK_QUEUE_NAME = "task_queue";

  public static void main(String[] argv) throws java.io.IOException
  {

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

    String option = "";
    while (true && !option.equals("exit")) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      option = reader.readLine();
      channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, option.getBytes());
      System.out.println(" [x] Sent '" + option + "'");
    }

    channel.close();
    connection.close();
  }



}
