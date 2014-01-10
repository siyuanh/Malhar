package com.datatorrent.contrib.dynamicpartition;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.Max;
import com.datatorrent.lib.testbench.RandomEventGenerator;

public class DynamicPartitionExample implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String type = conf.get("type");
    if(type == null){
      type = "parallel";
    }
    if(type.equalsIgnoreCase("parallel")){
      populateParallelDag(dag, conf);
    } else if(type.equalsIgnoreCase("nm")){
      populateNMDag(dag, conf);
    } else if(type.equalsIgnoreCase("unifier")){
      populateUnifierDag(dag, conf);
    }
    
  }

  private void populateUnifierDag(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "DynamicPartitionWithUnifier");
    
    RandomEventGenerator reg = dag.addOperator("Random Event Generator", new RandomEventGenerator());
    
    reg.setTuplesBlastIntervalMillis(1000);
    
    Max<Integer> max = new Max<Integer>();
    
    max = dag.addOperator("Max Unifier", max);
    
    ConsoleOutputOperator coo = dag.addOperator("Console Output", ConsoleOutputOperator.class);
    
    dag.addStream("reg to max", reg.integer_data, max.data);
    
    dag.addStream("max to console", max.max, coo.input);
    
    dag.setAttribute(max, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.setAttribute(max, OperatorContext.PARTITION_TPS_MAX, 2000);
    dag.setAttribute(max, OperatorContext.PARTITION_TPS_MIN, 1500);
    
    dag.setOutputPortAttribute(max.max, PortContext.UNIFIER_LIMIT, 2);
  }

  private void populateNMDag(DAG dag, Configuration conf)
  {
    String subType = conf.get("subtype");
    
    if(subType==null){
      subType = "half";
    }
    
    dag.setAttribute(DAG.APPLICATION_NAME, "DynamicPartitionNM");
    
    RandomEventGenerator reg = dag.addOperator("Random Event Generator", new RandomEventGenerator());
    
    reg.setTuplesBlastIntervalMillis(1000);
    
    reg.setTuplesBlast(3500);
    
    PipeOperator pipeline = dag.addOperator("Pipeline", PipeOperator.class);
    
    dag.setAttribute(pipeline, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    if(subType.equalsIgnoreCase("full")){
      // Enable dynamic partition for upper stream operator
      dag.setAttribute(pipeline, OperatorContext.PARTITION_TPS_MAX, 2000);
      dag.setAttribute(pipeline, OperatorContext.PARTITION_TPS_MIN, 1500);
    }
    
    dag.addStream("reg to pipeline", reg.integer_data, pipeline.dataIn);
    
    Max<Integer> max = new Max<Integer>();
    
    max = dag.addOperator("Max Unifier", max);
    
    dag.setAttribute(max, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    // Enable dynamic partition for down stream operator
    dag.setAttribute(max, OperatorContext.PARTITION_TPS_MAX, 2000);
    dag.setAttribute(max, OperatorContext.PARTITION_TPS_MIN, 1500);
    dag.setOutputPortAttribute(max.max, PortContext.UNIFIER_LIMIT, 2);
    
    dag.addStream("pipeline to max", pipeline.dataOut, max.data);
    
    ConsoleOutputOperator coo = dag.addOperator("Console Output", ConsoleOutputOperator.class);
    
    dag.addStream("max to console", max.max, coo.input);
    
  }

  private void populateParallelDag(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "DynamicParallelPartition");
    
    RandomEventGenerator reg = dag.addOperator("Random Event Generator", new RandomEventGenerator());
    
    reg.setTuplesBlastIntervalMillis(1000);
    
    PipeOperator pipeline = dag.addOperator("Pipeline", PipeOperator.class);
    
    ConsoleOutputOperator coo = dag.addOperator("Console Output", ConsoleOutputOperator.class);
    
    dag.addStream("reg to pipeline", reg.integer_data, pipeline.dataIn);
    
    dag.addStream("gto to coo", pipeline.dataOut, coo.input);
    
    dag.setAttribute(pipeline, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.setAttribute(pipeline, OperatorContext.PARTITION_TPS_MAX, 2000);
    dag.setAttribute(pipeline, OperatorContext.PARTITION_TPS_MIN, 1000);
    
    dag.setInputPortAttribute(coo.input, PortContext.PARTITION_PARALLEL, true);
    
  }

}
