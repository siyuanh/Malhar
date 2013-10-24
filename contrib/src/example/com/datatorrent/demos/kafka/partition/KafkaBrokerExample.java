package com.datatorrent.demos.kafka.partition;

import com.datatorrent.contrib.kafka.KafkaOperatorTestBase;

public class KafkaBrokerExample
{

  public static void main(String[] args)
  {
    KafkaOperatorTestBase kotb = new KafkaOperatorTestBase();
    kotb.setHasMultiPartition(true);
    kotb.beforeTest(); 
    System.out.println();
  }

}
