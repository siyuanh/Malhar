package com.datatorrent.demos.kafka.partition;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;

public class KafkaOutputBenchmark implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration arg1)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "KafkaProducerBenchmark");
    BenchmarkPartitionableKafkaOutputOperator bpkoo = dag.addOperator("HighThoughputKafkaOutput", BenchmarkPartitionableKafkaOutputOperator.class);
    dag.setAttribute(bpkoo, OperatorContext.INITIAL_PARTITION_COUNT, 2);
  }

}
