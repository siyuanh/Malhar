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

import org.junit.After;
import org.junit.Before;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoDBTestBase
{

  protected static String TEST_HOSTNAME = "localhost";
  
  protected static String TEST_DB = "test";
  
  protected static String TEST_USER = "test";
  
  protected static String TEST_PWD = "123";
  
  protected MongoDBStore createTestStore()
  {
    MongoDBStore store = new MongoDBStore();
    store.setHostname(TEST_HOSTNAME);
    store.setDataBase(TEST_DB);
    store.setUserName(TEST_USER);
    store.setPassWord(TEST_PWD);
    store.setWindowIdColumnName("winid");
    store.setOperatorIdColumnName("operatorid");
    store.setMaxWindowTable("maxWindowTable");
    store.setAppIdNameColumnName("testApp");
    return store;
  }

  @After
  public void cleanTestEnv()
  {
    try {
      MongoClient mongoClient = new MongoClient(TEST_HOSTNAME);
      DB db = mongoClient.getDB(TEST_DB);
      // drop the test database
      db.dropDatabase();
      mongoClient.fsync(false);
      mongoClient.close();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }
  
  
  @Before
  public void prepareTestEnv(){
    
    try {
      MongoClient mongoClient = new MongoClient(TEST_HOSTNAME);
      // create test database and test username with test password
      DB db = mongoClient.getDB(TEST_DB);
      db.addUser(TEST_USER, TEST_PWD.toCharArray());
      mongoClient.fsync(false);
      mongoClient.close();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    
  }

}
