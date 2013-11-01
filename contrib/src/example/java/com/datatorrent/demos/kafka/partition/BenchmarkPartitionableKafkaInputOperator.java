package com.datatorrent.demos.kafka.partition;

import kafka.message.Message;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.kafka.AbstractPartitionableKafkaInputOperator;

public class BenchmarkPartitionableKafkaInputOperator extends AbstractPartitionableKafkaInputOperator
{

  public transient DefaultOutputPort<String>  oport = new DefaultOutputPort<String>();
  
  @Override
  protected AbstractPartitionableKafkaInputOperator cloneOperator()
  {
    return new BenchmarkPartitionableKafkaInputOperator();
  }

  @Override
  protected void emitTuple(Message message)
  {
    oport.emit("Received");
  }

}
