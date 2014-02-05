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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.*;

import com.mongodb.DBCursor;
import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 *
 */
public class MongoDBOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBOutputOperatorTest.class);
  public String[] hashMapping1 = new String[columnNum];
  public String[] arrayMapping1 = new String[columnNum];
  public final static int maxTuple = 20;
  public final static int columnNum = 5;
  public AttributeMap attrmap = new DefaultAttributeMap();
  private MongoDBTestHelper helper = new MongoDBTestHelper();
  private Set<Map<String, Object>> expectedData = new HashSet<Map<String, Object>>(); 

  public void buildDataset()
  {
//    hashMapping1[0] = "prop1:t1.col1:STRING";
//    hashMapping1[1] = "prop2:t1.col2:STRING";
//    hashMapping1[2] = "prop5:t1.col5:STRING";
//    hashMapping1[3] = "prop6:t1.col4:STRING";
//    hashMapping1[4] = "prop7:t1.col7:STRING";

    expectedData.clear();
    hashMapping1[0] = "prop1:t1.col1:INT";
    hashMapping1[1] = "prop3:t1.col3:STRING";
    hashMapping1[2] = "prop2:t1.col2:DATE";
    hashMapping1[3] = "prop4:t2.col1:STRING";
    hashMapping1[4] = "prop5:t2.col2:INT";

    arrayMapping1[0] = "t1.col1:INT";
    arrayMapping1[1] = "t1.col3:STRING";
    arrayMapping1[2] = "t1.col2:DATE";
    arrayMapping1[3] = "t2.col2:STRING";
    arrayMapping1[4] = "t2.col1:INT";

    attrmap.put(DAG.APPLICATION_ID, "myMongoDBOouputOperatorAppId");

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public HashMap<String, Object> generateHashMapData(int j, MongoDBHashMapOutputOperator oper)
  {
    HashMap<String, Object> hm = new HashMap<String, Object>();
    Map<String, Map<String, Object>> records = new HashMap<String, Map<String,Object>>();
    records.put("t1", new HashMap<String, Object>());
    records.put("t2", new HashMap<String, Object>());
    for (int i = 0; i < columnNum; i++) {
      String[] tokens = hashMapping1[i].split("[:]");
      String[] subtok = tokens[1].split("[.]");
      String table = subtok[0];
      String col = subtok[1];
      String prop = tokens[0];
      String type = tokens[2];
      if (type.contains("INT")) {
        hm.put(prop, j * columnNum + i);
        records.get(table).put(col, j*columnNum+i);
      }
      else if (type.equals("STRING")) {
        hm.put(prop, String.valueOf(j * columnNum + i));
        records.get(table).put(col, String.valueOf(j*columnNum+i));
      }
      else if (type.equals("DATE")) {
        Date data = new Date();
        hm.put(prop, data);
        records.get(table).put(col, data);
      }
      oper.propTableMap.put(prop, table);
    }
    hm.put("sumup", records);
    return hm;
  }

  public ArrayList<Object> generateArrayListData(int j, MongoDBArrayListOutputOperator oper) {
    ArrayList<Object> al = new ArrayList<Object>();
    Map<String, Map<String, Object>> records = new HashMap<String, Map<String,Object>>();
    records.put("t1", new HashMap<String, Object>());
    records.put("t2", new HashMap<String, Object>());
    for( int i=0; i< columnNum; i++ ) {
      String[] tokens = arrayMapping1[i].split("[:]");
      String[] tC = tokens[0].split("[.]");
      String type = tokens[1];
      if (type.contains("INT")) {
        al.add(j*columnNum+i);
        records.get(tC[0]).put(tC[1], j*columnNum+i);
      }
      else if (type.equals("STRING")) {
        al.add(String.valueOf(j*columnNum+i));
        records.get(tC[0]).put(tC[1], String.valueOf(j*columnNum+i));
      }
      else if (type.equals("DATE")) {
        Date data = new Date();
        al.add(data);
        records.get(tC[0]).put(tC[1], data);
      }
    }
    al.add(records);
    return al;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void readDB(AbstractMongoDBOutputOperator oper)
  {
    Set<Map<String, Object>> realValue = new HashSet<Map<String, Object>>();
    for (Object o : oper.getTableList()) {
      String table = (String)o;
      DBCursor cursor = ((MongoDBStore)oper.getStore()).getCollection(table).find();
      while (cursor.hasNext()) {
        
        Map<String, Object> dbRecord = cursor.next().toMap();
        dbRecord.remove("_id");
        realValue.add(dbRecord);
      }
//      assertTrue(Arrays.equals(expectedData, realValue));
    }
    assertEquals(expectedData, realValue);
  }
  @SuppressWarnings("unchecked")
  @Test
  public void MongoDBHashMapOutputOperatorTest()
  {
    buildDataset();

    MongoDBHashMapOutputOperator<Object> oper = new MongoDBHashMapOutputOperator<Object>();

    MongoDBStore store =  helper.getTestStore();
    oper.setBatchSize(3);
    oper.setQueryFunction(1);
    oper.setColumnMapping(hashMapping1);
    oper.setStore(store);

    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));

    for (Object o : oper.getTableList()) {
      String table = (String)o;
      store.getCollection(table).drop();
    }

    oper.beginWindow(oper.getLastWindowId() + 1);
    logger.debug("beginwindow {}", oper.getLastWindowId() + 1);

    for (int i = 0; i < maxTuple; ++i) {
      HashMap<String, Object> hm = generateHashMapData(i, oper);
//      logger.debug(hm.toString());
      Map<String, Map<String, Object>> expectR = (Map<String, Map<String, Object>>) hm.remove("sumup");
      expectedData.add(expectR.get("t1"));
      expectedData.add(expectR.get("t2"));
      oper.input.process(hm);

    }
    oper.endWindow();
    readDB(oper);

    oper.teardown();
  }
  @SuppressWarnings("unchecked")
  @Test
  public void MongoDBArrayListOutputOperatorTest() {
    buildDataset();
    MongoDBArrayListOutputOperator oper = new MongoDBArrayListOutputOperator();

    MongoDBStore store =  helper.getTestStore(); 
    oper.setBatchSize(3);
    oper.setQueryFunction(1);
    oper.setColumnMapping(arrayMapping1);
    oper.setStore(store);

    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(2));
    for (Object o : oper.getTableList()) {
      String table = (String)o;
      store.getCollection(table).drop();
    }

    oper.beginWindow(oper.getLastWindowId() + 1);
    logger.debug("beginwindow {}", oper.getLastWindowId() + 1);

    for (int i = 0; i < maxTuple; ++i) {
      ArrayList<Object> al = generateArrayListData(i, oper);
      Map<String, Map<String, Object>> expectR = (Map<String, Map<String, Object>>) al.remove(al.size()-1);
      expectedData.add(expectR.get("t1"));
      expectedData.add(expectR.get("t2"));
      oper.input.process(al);
    }
    oper.endWindow();
    readDB(oper);

    oper.teardown();
  }



}
