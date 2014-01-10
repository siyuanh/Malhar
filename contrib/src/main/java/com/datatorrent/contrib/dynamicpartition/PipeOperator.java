package com.datatorrent.contrib.dynamicpartition;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class PipeOperator extends BaseOperator
{

  public final transient DefaultOutputPort<Integer> dataOut = new DefaultOutputPort<Integer>();
  
  public final transient DefaultInputPort<Integer> dataIn = new DefaultInputPort<Integer>(){

    @Override
    public void process(Integer v)
    {
      dataOut.emit(v);
    }
    
  };
  

}
