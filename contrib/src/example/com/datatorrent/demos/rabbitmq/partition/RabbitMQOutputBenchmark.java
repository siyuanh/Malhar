package com.datatorrent.demos.rabbitmq.partition;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;

public class RabbitMQOutputBenchmark implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    BenchmarkPartitionableRabbitMQOutputOperator bprmo = dag.addOperator("HighThoughputRabbitInput", BenchmarkPartitionableRabbitMQOutputOperator.class);
    dag.setAttribute(bprmo, OperatorContext.INITIAL_PARTITION_COUNT, 1);
  }

}
