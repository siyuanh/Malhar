/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.mongodb;


import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.mongodb.DBCursor;

/**
 *
 */
public class MongoDBInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBInputOperatorTest.class);
  public String[] hashMapping1 = new String[columnNum];
  public String[] arrayMapping1 = new String[columnNum];
  public final static int maxTuple = 20;
  public final static int columnNum = 5;
  private MongoDBTestHelper helper = new MongoDBTestHelper();

  public class MyMongoDBInputOperator extends AbstractMongoDBInputOperator<Object>
  {
    @Override
    public Object getTuple(DBCursor result)
    {
      while(result.hasNext()) {
        System.out.println(result.next().toString());
      }
      return result;
    }

    @Override
    public void processTuple(Object tuple)
    {
      // TODO Auto-generated method stub
      
    }
  };

  @Test
  public void MongoDBInputOperatorTest()
  {
    MyMongoDBInputOperator oper = new MyMongoDBInputOperator();

    MongoDBStore store = helper.getTestStore();
    oper.setTable("t1");
    oper.setStore(store);

//    AttributeMap<DAGContext> attrmap = new DefaultAttributeMap<DAGContext>();
//    attrmap.attr(DAG.STRAM_APP_ID).set("myMongoDBInputOperatorAppId");
    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));

    oper.beginWindow(0);

    CollectorTestSink sink = new CollectorTestSink();
    oper.outputPort.setSink(sink);

    oper.emitTuples();

    oper.endWindow();

    oper.teardown();
  }
}
