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

import javax.annotation.Nonnull;

import com.datatorrent.lib.database.AbstractDBLookupCacheBackedOperator;
import com.datatorrent.lib.database.DBConnector;

/**
 * <br>This is {@link AbstractDBLookupCacheBackedOperator} which retrieves value of a key
 * from MongoDB</br>
 *
 * @param <T> type of input tuples </T>
 * @since 0.9.1
 */
public abstract class MongoDBLookupCacheBackedOperator<T> extends AbstractDBLookupCacheBackedOperator<T>
{

  public MongoDBLookupCacheBackedOperator()
  {
    super();
  }

  @Nonnull
  @Override
  public DBConnector getDbConnector()
  {
    return null;
  }

  /**
   * Sets the username which is used to connect to mongoDB.
   *
   * @param userName user name
   */
  public void setUserName(String userName)
  {
  }

  /**
   * Sets the password of mongoDB user.
   *
   * @param passWord password
   */
  public void setPassWord(String passWord)
  {
  }

  /**
   * Sets the database which will be used.
   *
   * @param dataBase database name
   */
  public void setDataBase(String dataBase)
  {
  }

  /**
   * Sets the host of mongoDB.
   *
   * @param hostName host name of mongo db
   */
  public void setHostName(String hostName)
  {
  }
}
