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


import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import static junit.framework.Assert.*;

/**
 *
 */
public class MongoDBInputOperatorTest extends MongoDBTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBInputOperatorTest.class);
  public String[] hashMapping1 = new String[columnNum];
  public String[] arrayMapping1 = new String[columnNum];
  public final static int maxTuple = 20;
  public final static int columnNum = 5;
  private final Set<Map<String, Object>> expectedData = new HashSet<Map<String,Object>>();

  @SuppressWarnings("rawtypes")
  public class MyMongoDBInputOperator extends AbstractMongoDBInputOperator<Map>
  {
    @Override
    public Map getTuple(DBObject result)
    {
      Map tuple = result.toMap();
      tuple.remove("_id");
      return tuple;
    }
    
  };

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testMongoDBInputOperator()
  {
    
    prepareData();
    
    MyMongoDBInputOperator oper = new MyMongoDBInputOperator();

    MongoDBStore store = createTestStore();
    oper.setTable("testCollection");
    oper.setStore(store);

//    AttributeMap<DAGContext> attrmap = new DefaultAttributeMap<DAGContext>();
//    attrmap.attr(DAG.STRAM_APP_ID).set("myMongoDBInputOperatorAppId");
    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));

    oper.beginWindow(0);

    CollectorTestSink sink = new CollectorTestSink();
    oper.outputPort.setSink(sink);

    oper.emitTuples();

    assertEquals(expectedData, Sets.newHashSet(sink.collectedTuples));
    
    oper.endWindow();

    oper.teardown();
  }

  private void prepareData()
  {
    expectedData.clear();
    try {
      MongoClient mongoClient = new MongoClient(TEST_HOSTNAME);
      DB db = mongoClient.getDB(TEST_DB);
      // drop the test database
      db.authenticate(TEST_USER, TEST_PWD.toCharArray());
      DBCollection dbc = db.createCollection("testCollection", null);
      
      List<DBObject> datas = new LinkedList<DBObject>();
      for (int i = 0; i < maxTuple; i++) {
        Map<String, Object> record = new HashMap<String, Object>();
        record.put("col1", RandomUtils.nextInt(1000));
        record.put("col2", RandomStringUtils.randomAlphabetic(5));
        datas.add(new BasicDBObject(record));
        expectedData.add(record);
      }
      dbc.insert(datas);
      mongoClient.fsync(false);
      mongoClient.close();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    
  }
}
